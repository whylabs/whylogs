import logging
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from whylogs.core.datatypes import DataType, StandardTypeMapper, TypeMapper
from whylogs.core.metrics.metrics import Metric, MetricConfig
from whylogs.core.resolvers import NO_FI_RESOLVER, MetricSpec, ResolverSpec
from whylogs.core.schema import DeclarativeSchema
from whylogs.core.segmentation_partition import SegmentationPartition
from whylogs.core.stubs import pd
from whylogs.core.validators.validator import Validator
from whylogs.experimental.core.metrics.udf_metric import (
    _reset_metric_udfs,
    generate_udf_resolvers,
)
from whylogs.experimental.core.validators.validator import generate_validators

logger = logging.getLogger(__name__)


@dataclass
class UdfSpec:
    """
    Defines UDFs to apply to matching input columns.

    For UDFs matched by column_name(s), the function is passed a dictionary or
    dataframe with the named columns available (the UDF will not be called unless
    all the named columns are available). The output column name is the key in
    the udfs dictionary.

    For UDFs matched by column_type, the function is passed the value or Pandas
    series. The output column name is the key in the udfs dictionary prefixed
    by the input column name.

    You must specify exactly one of column_names or column_type.
    """

    column_names: Optional[List[str]] = None
    udfs: Dict[str, Callable[[Any], Any]] = field(
        default_factory=dict
    )  # new column name -> callable to compute new column value
    column_type: Optional[DataType] = None
    prefix: Optional[str] = None

    # for multiple output column UDFs
    udf: Optional[Callable[[Any], Any]] = None
    name: Optional[str] = None

    def __post_init__(self):
        if self.column_type is not None:
            if self.column_names:
                raise ValueError("UdfSpec cannot specify both column_names and column_type")
        elif self.column_names is None:
            raise ValueError("UdfSpec must specify column_names or column_type")
        elif len(self.column_names) == 0 or not all([isinstance(x, str) for x in self.column_names]):
            raise ValueError("UdfSpec column_names must be a non-empty list of strings")


def _apply_udfs_on_row(
    values: Union[List, Dict[str, List]], udfs: Dict, new_columns: Dict[str, Any], input_cols: Collection[str]
) -> None:
    """multiple input columns, single output column"""
    for new_col, udf in udfs.items():
        if new_col in input_cols:
            continue

        try:
            new_columns[new_col] = udf(values)[0]
        except Exception:  # noqa
            new_columns[new_col] = None
            logger.exception(f"Evaluating UDF {new_col} failed")


def _apply_udf_on_row(
    name: str,
    prefix: Optional[str],
    values: Union[List, Dict[str, List]],
    udf: Callable,
    new_columns: Dict[str, Any],
    input_cols: Collection[str],
) -> None:
    """
    multiple input columns, multiple output columns
    udf(Union[Dict[str, List], pd.DataFrame]) -> Union[Dict[str, List], pd.DataFrame]
    """

    try:
        # TODO: Document assumption: dictionary in -> dictionary out
        for new_col, value in udf(values).items():
            new_col = prefix + "." + new_col if prefix else new_col
            new_columns[new_col] = value[0]

    except Exception as e:  # noqa
        logger.exception(f"Evaluating UDF {name} failed with error {e}")


def _apply_udfs_on_dataframe(
    pandas: pd.DataFrame, udfs: Dict, new_df: pd.DataFrame, input_cols: Collection[str]
) -> None:
    """multiple input columns, single output column"""
    for new_col, udf in udfs.items():
        if new_col in input_cols:
            continue

        try:
            new_df[new_col] = pd.Series(udf(pandas))
        except Exception as e:  # noqa
            new_df[new_col] = pd.Series([None])
            logger.exception(f"Evaluating UDF {new_col} failed on columns {pandas.keys()} with error {e}")


def _apply_udf_on_dataframe(
    name: str,
    prefix: Optional[str],
    pandas: pd.DataFrame,
    udf: Callable,
    new_df: pd.DataFrame,
    input_cols: Collection[str],
) -> None:
    """
    multiple input columns, multiple output columns
    udf(Union[Dict[str, List], pd.DataFrame]) -> Union[Dict[str, List], pd.DataFrame]
    """

    def add_prefix(col):
        return prefix + "." + col if prefix else col

    try:
        # TODO: I think it's OKAY if udf returns a dictionary
        udf_output = pd.DataFrame(udf(pandas))
        udf_output = udf_output.rename(columns={old: add_prefix(old) for old in udf_output.keys()})
        for new_col in udf_output.keys():
            new_df[new_col] = udf_output[new_col]
    except Exception as e:  # noqa
        logger.exception(f"Evaluating UDF {name} failed on columns {pandas.keys()} with error {e}")
        return pd.DataFrame()


def _apply_type_udfs(pandas: pd.Series, udfs: Dict, new_df: pd.DataFrame, input_cols: Collection[str]) -> None:
    for new_col, udf in udfs.items():
        if new_col in input_cols:
            continue

        try:
            new_df[new_col] = pd.Series(udf(pandas))
        except Exception as e:  # noqa
            new_df[new_col] = pd.Series([None])
            logger.exception(f"Evaluating UDF {new_col} failed on column {new_col} with error {e}")


class UdfSchema(DeclarativeSchema):
    """
    Subclass of DeclarativeSchema that runs the UDFs specified in udf_specs to
    create new columns before resolving metrics.
    """

    def __init__(
        self,
        resolvers: Optional[List[ResolverSpec]] = None,
        types: Optional[Dict[str, Any]] = None,
        default_config: Optional[MetricConfig] = None,
        type_mapper: Optional[TypeMapper] = None,
        cache_size: int = 1024,
        schema_based_automerge: bool = False,
        segments: Optional[Dict[str, SegmentationPartition]] = None,
        validators: Optional[Dict[str, List[Validator]]] = None,
        udf_specs: Optional[List[UdfSpec]] = None,
        drop_columns: Optional[Set[str]] = None,
    ) -> None:
        super().__init__(
            resolvers=resolvers,
            types=types,
            default_config=default_config,
            type_mapper=type_mapper,
            cache_size=cache_size,
            schema_based_automerge=schema_based_automerge,
            segments=segments,
            validators=validators,
        )
        self.drop_columns = drop_columns if drop_columns else set()
        udf_specs = udf_specs if udf_specs else []
        self.multicolumn_udfs = [spec for spec in udf_specs if spec.column_names]
        self.type_udfs = defaultdict(list)
        for spec in udf_specs:
            if spec.column_type:
                self.type_udfs[spec.column_type].append(spec)

    def copy(self) -> "UdfSchema":
        copy = super().copy()
        copy.multicolumn_udfs = deepcopy(self.multicolumn_udfs)
        copy.type_udfs = deepcopy(self.type_udfs)
        return copy

    def _run_udfs_on_row(
        self, row: Mapping[str, Any], new_columns: Dict[str, Any], input_cols: Collection[str]
    ) -> None:
        for spec in self.multicolumn_udfs:
            if spec.column_names and set(spec.column_names).issubset(set(row.keys())):
                inputs = {col: [row[col]] for col in spec.column_names}
                if spec.udf is not None:
                    _apply_udf_on_row(spec.name, spec.prefix, inputs, spec.udf, new_columns, input_cols)  # type: ignore
                else:
                    _apply_udfs_on_row(inputs, spec.udfs, new_columns, input_cols)

        for column, value in row.items():
            why_type = type(self.type_mapper(type(value)))
            for spec in self.type_udfs[why_type]:
                udfs = {f"{column}.{key}": spec.udfs[key] for key in spec.udfs.keys()}
                _apply_udfs_on_row([value], udfs, new_columns, input_cols)

    def _run_udfs_on_dataframe(self, pandas: pd.DataFrame, new_df: pd.DataFrame, input_cols: Collection[str]) -> None:
        for spec in self.multicolumn_udfs:
            if spec.column_names and set(spec.column_names).issubset(set(pandas.keys())):
                if spec.udf is not None:
                    _apply_udf_on_dataframe(
                        spec.name, spec.prefix, pandas[spec.column_names], spec.udf, new_df, input_cols  # type: ignore
                    )
                else:
                    _apply_udfs_on_dataframe(pandas[spec.column_names], spec.udfs, new_df, input_cols)

        for column, dtype in pandas.dtypes.items():
            why_type = type(self.type_mapper(dtype))
            for spec in self.type_udfs[why_type]:
                udfs = {f"{column}.{key}": spec.udfs[key] for key in spec.udfs.keys()}
                _apply_type_udfs(pandas[column], udfs, new_df, input_cols)

    def _run_udfs(
        self, pandas: Optional[pd.DataFrame] = None, row: Optional[Dict[str, Any]] = None
    ) -> Tuple[Optional[pd.DataFrame], Optional[Mapping[str, Any]]]:
        new_columns = deepcopy(row) if row else None
        new_df = pd.DataFrame()
        if row is not None:
            self._run_udfs_on_row(row, new_columns, row.keys())  # type: ignore
            if self.drop_columns:
                for col in set(row.keys()).intersection(self.drop_columns):
                    row.pop(col)

        if pandas is not None:
            self._run_udfs_on_dataframe(pandas, new_df, pandas.keys())
            new_df = pd.concat([pandas, new_df], axis=1)
            if self.drop_columns:
                new_df = new_df.drop(columns=list(set(new_df.keys()).intersection(self.drop_columns)))

        return new_df if pandas is not None else None, new_columns

    def apply_udfs(
        self, pandas: Optional[pd.DataFrame] = None, row: Optional[Dict[str, Any]] = None
    ) -> Tuple[Optional[pd.DataFrame], Optional[Mapping[str, Any]]]:
        return self._run_udfs(pandas, row)


_multicolumn_udfs: Dict[str, List[UdfSpec]] = defaultdict(list)
_resolver_specs: Dict[str, List[ResolverSpec]] = defaultdict(list)


def _reset_udfs(reset_metric_udfs: bool = True) -> None:
    if reset_metric_udfs:
        _reset_metric_udfs()

    global _multicolumn_udfs, _resolver_specs
    _multicolumn_udfs = defaultdict(list)
    _resolver_specs = defaultdict(list)


def register_multioutput_udf(
    col_names: List[str],
    udf_name: Optional[str] = None,
    prefix: Optional[str] = None,
    namespace: Optional[str] = None,
    schema_name: str = "",
    no_prefix: bool = False,
) -> Callable[[Any], Any]:
    """
    Decorator to easily configure UDFs for your data set. Decorate your UDF
    functions, then call generate_udf_dataset_schema() to create a UdfSchema
    that includes the UDFs configured by your decorator parameters. The decorated
    function will automatically be a UDF in the UdfSchema.

    Specify udf_name to give the output of the UDF a name. udf_name
    defautls to the name of the decorated function. Note that all lambdas are
    named "lambda", so omitting udf_name on more than one lambda will result
    in name collisions. If you pass a namespace, it will be prepended to the UDF name.
    Specifying schema_name will register the UDF in a particular schema. If omitted,
    it will be registered to the defualt schema.

    For multiple output column UDFs, the udf_name is prepended to the column
    name supplied by the UDF. The signature for multiple output column UDFs
    is f(Union[Dict[str, List], pd.DataFrame]) -> Union[Dict[str, List], pd.DataFrame]
    """

    def decorator_register(func):
        global _multicolumn_udfs
        name = udf_name or func.__name__
        name = f"{namespace}.{name}" if namespace else name
        if no_prefix:
            output_prefix = None
        else:
            output_prefix = prefix if prefix else name
        _multicolumn_udfs[schema_name].append(UdfSpec(col_names, prefix=output_prefix, udf=func, name=name))
        return func

    return decorator_register


def register_dataset_udf(
    col_names: List[str],
    udf_name: Optional[str] = None,
    metrics: Optional[List[MetricSpec]] = None,
    namespace: Optional[str] = None,
    schema_name: str = "",
    anti_metrics: Optional[List[Metric]] = None,
) -> Callable[[Any], Any]:
    """
    Decorator to easily configure UDFs for your data set. Decorate your UDF
    functions, then call generate_udf_dataset_schema() to create a UdfSchema
    that includes the UDFs configured by your decorator parameters. The decorated
    function will automatically be a UDF in the UdfSchema.

    Specify udf_name to give the output of the UDF a name. udf_name
    defautls to the name of the decorated function. Note that all lambdas are
    named "lambda", so omitting udf_name on more than one lambda will result
    in name collisions. If you pass a namespace, it will be prepended to the UDF name.
    Specifying schema_name will register the UDF in a particular schema. If omitted,
    it will be registered to the defualt schema.

    If any metrics are passed via the metrics argument, they will be attached
    to the column produced by the UDF via the schema returned by generate_udf_dataset_schema().
    If metrics is None, the UDF output column will get the metrics determined by
    the other resolvers passed to generate_udf_dataset_schema(), or the STANDARD_UDF_RESOLVER
    by default. Any anti_metrics will be excluded from the metrics attached to the UDF output.
    """

    def decorator_register(func):
        global _multicolumn_udfs, _resolver_specs
        name = udf_name or func.__name__
        name = f"{namespace}.{name}" if namespace else name
        _multicolumn_udfs[schema_name].append(UdfSpec(col_names, {name: func}))
        if metrics:
            _resolver_specs[schema_name].append(ResolverSpec(name, None, deepcopy(metrics)))
        if anti_metrics:
            _resolver_specs[schema_name].append(ResolverSpec(name, None, [MetricSpec(m) for m in anti_metrics], True))

        return func

    return decorator_register


def unregister_udf(udf_name: str, namespace: Optional[str] = None, schema_name: str = "") -> None:
    global _multicolumn_udfs, _resolver_specs
    name = f"{namespace}.{udf_name}" if namespace else udf_name
    if schema_name not in _multicolumn_udfs:
        logger.warn(f"Can't unregister UDF {name} from non-existant schema {schema_name}")
        return

    found = False
    for spec in _multicolumn_udfs[schema_name]:
        if name in spec.udfs:
            found = True
            del spec.udfs[name]
    if not found:
        logger.warn(f"UDF {name} could not be found for unregistering")
    _resolver_specs[schema_name] = list(filter(lambda x: x.column_name != name, _resolver_specs[schema_name]))


def register_type_udf(
    col_type: Type,
    udf_name: Optional[str] = None,
    namespace: Optional[str] = None,
    schema_name: str = "",
    type_mapper: Optional[TypeMapper] = None,
) -> Callable[[Any], Any]:
    """
    Decorator to easily configure UDFs for your data set. Decorate your UDF
    functions, then call generate_udf_dataset_schema() to create a UdfSchema
    that includes the UDFs configured by your decorator parameters. The decorated
    function will automatically be a UDF in the UdfSchema.

    The registered function will be applied to any columns of the specified type.
    Specify udf_name to give the output of the UDF a name. udf_name
    defautls to the name of the decorated function. The output column name is the
    UDF name prefixed with the input column name. Note that all lambdas are
    named "lambda", so omitting udf_name on more than one lambda will result
    in name collisions. If you pass a namespace, it will be prepended to the UDF name.
    Specifying schema_name will register the UDF in a particular schema. If omitted,
    it will be registered to the defualt schema.

    """
    try:
        if not issubclass(col_type, DataType):
            type_mapper = type_mapper or StandardTypeMapper()
            col_type = type(type_mapper(col_type))
    except:  # noqa
        raise ValueError("Column type must be a DataType or Python type mapable to one")

    def decorator_register(func):
        global _multicolumn_udfs, _resolver_specs
        name = udf_name or func.__name__
        name = f"{namespace}.{name}" if namespace else name
        _multicolumn_udfs[schema_name].append(UdfSpec(None, {name: func}, col_type))

        return func

    return decorator_register


def generate_udf_specs(
    other_udf_specs: Optional[List[UdfSpec]] = None,
    schema_name: Union[str, List[str]] = "",
    include_default_schema: bool = True,
) -> List[UdfSpec]:
    """
    Generates a list of UdfSpecs that implement the UDFs specified by the
    @register_dataset_udf, @register_type_udf, and @register_metric_udf
    decorators. You can provide a list of other_udf_specs to include in
    addition to those UDFs registered via the decorator.

    For example:

    @register_dataset_udf(col_names=["col1"])
    def add5(x):
        return x + 5

    schema = UdfSchema(STANDARD_RESOLVER, udf_specs=generate_udf_specs())
    why.log(data, schema=schema)

    This will attach a UDF to column "col1" that will generate a new column
    named "add5" containing the values in "col1" incremented by 5. Since these
    are appended to the STANDARD_UDF_RESOLVER, the default metrics are also tracked
    for every column.
    """
    specs = list(other_udf_specs) if other_udf_specs else []
    schema_name = schema_name if isinstance(schema_name, list) else [schema_name]
    if include_default_schema and "" not in schema_name:
        schema_name = [""] + schema_name

    for name in schema_name:
        specs += _multicolumn_udfs[name]
    return specs


DEFAULT_UDF_SCHEMA_RESOLVER = NO_FI_RESOLVER


def udf_schema(
    other_udf_specs: Optional[List[UdfSpec]] = None,
    resolvers: Optional[List[ResolverSpec]] = None,
    types: Optional[Dict[str, Any]] = None,
    default_config: Optional[MetricConfig] = None,
    type_mapper: Optional[TypeMapper] = None,
    cache_size: int = 1024,
    schema_based_automerge: bool = False,
    segments: Optional[Dict[str, SegmentationPartition]] = None,
    validators: Optional[Dict[str, List[Validator]]] = None,
    schema_name: Union[str, List[str]] = "",
    include_default_schema: bool = True,
    drop_columns: Optional[Set[str]] = None,
) -> UdfSchema:
    """
    Returns a UdfSchema that implements any registered UDFs, along with any
    other_udf_specs or resolvers passed in.
    """
    resolver_specs = list(resolvers if resolvers is not None else DEFAULT_UDF_SCHEMA_RESOLVER)
    schema_names = schema_name if isinstance(schema_name, list) else [schema_name]
    if include_default_schema and "" not in schema_names:
        schema_names = [""] + schema_names

    for name in schema_names:
        resolver_specs += _resolver_specs[name]
        validators = generate_validators(validators, name, include_default_schema=True)

    resolver_specs += generate_udf_resolvers(schema_name, include_default_schema)
    return UdfSchema(
        resolver_specs,
        types,
        default_config,
        type_mapper,
        cache_size,
        schema_based_automerge,
        segments,
        validators,
        generate_udf_specs(other_udf_specs, schema_name, include_default_schema),
        drop_columns,
    )
