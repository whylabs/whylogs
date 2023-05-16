import logging
import sys
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Union

from whylogs.core.datatypes import DataType, TypeMapper
from whylogs.core.metrics.metrics import MetricConfig
from whylogs.core.resolvers import STANDARD_RESOLVER, MetricSpec, ResolverSpec
from whylogs.core.schema import DatasetSchema, DeclarativeSchema
from whylogs.core.segmentation_partition import SegmentationPartition
from whylogs.core.stubs import pd
from whylogs.core.validators.validator import Validator

logger = logging.getLogger(__name__)


@dataclass
class UdfSpec:
    """
    Defines UDFs to apply to matching input columns. If column_name is a string,
    the udfs will be applied to the named columns. The UDFs are passed the single
    value in the column as input. If column_name is a list of strings, the UDF is
    passed a dictionary or dataframe with the named columns available (the UDF will
    not be called unless all the named columns are available). If column_name is None,
    input columns can be matched by column_type.

    In the cases where there's a single input column (column_name is a stirng or
    matching by column_type), the new column name is f"{input_column_name}.{udf_key}",
    where udf_key is the key in the dictionary of UDFs. If column_name is a list of
    stings (even of length 1), the new column name is just the udf_key.
    """

    column_name: Optional[Union[str, List[str]]] = None
    column_type: Optional[DataType] = None
    udfs: Dict[str, Callable[[Any], Any]] = field(
        default_factory=dict
    )  # new column name -> callable to compute new column value

    def __post_init__(self):
        if isinstance(self.column_name, list) and (
            len(self.column_name) == 0 or not all([isinstance(x, str) for x in self.column_name])
        ):
            raise ValueError("UdfSpec: column name must be either a single string or a non-empty list of strings")
        if self.column_name and self.column_type:
            logger.warning(f"UdfSpec: column(s) {self.column_name} also specified type, names takes precedence")
        if not (self.column_name or self.column_type):
            raise ValueError("UdfSpec: specification must supply name(s) or type")

        if self.column_type and not issubclass(self.column_type, DataType):
            raise ValueError("UdfSpec: column type must be a DataType")


def _apply_udfs_on_row(value: Any, column: Optional[str], udfs: Dict, new_columns: Dict[str, Any]) -> None:
    if column is None:  # multi-column UDF
        for new_col, udf in udfs.items():
            try:
                new_columns[new_col] = udf(value)
            except Exception:  # noqa
                logger.exception(f"Evaluating multi-column UDF {new_col} failed")
        return

    for new_col, udf in udfs.items():
        if new_col in new_columns:
            logger.warning(f"UDF {udf.__name__} overwriting column {new_col}")
        try:
            new_columns[f"{column}.{new_col}"] = udf(value)  # type: ignore
        except Exception:  # noqa
            logger.exception(f"Evaluating UDF {new_col} failed on column {column}")


def _apply_udfs_on_dataframe(pandas: pd.DataFrame, column: Optional[str], udfs: Dict, new_df: pd.DataFrame) -> None:
    if column is None:  # multi-column UDF
        for new_col, udf in udfs.items():
            try:
                new_df[new_col] = udf(pandas)
            except Exception:  # noqa
                logger.exception(f"Evaluating UDF {new_col} failed on columns {pandas.keys()}")
        return

    for new_col, udf in udfs.items():
        if new_col in new_df.keys():
            logger.warning(f"UDF {udf.__name__} overwriting column {new_col}")
        try:
            new_df[f"{column}.{new_col}"] = pandas[column].map(udf)
        except Exception:  # noqa
            logger.exception(f"Evaluating UDF {new_col} failed on column {column}")


class UdfSchema(DeclarativeSchema):
    """
    Subclass of DeclarativeSchema that runs the UDFs specified in udf_specs to
    create new columns before resolving metrics.
    """

    def __init__(
        self,
        resolvers: List[ResolverSpec],
        types: Optional[Dict[str, Any]] = None,
        default_config: Optional[MetricConfig] = None,
        type_mapper: Optional[TypeMapper] = None,
        cache_size: int = 1024,
        schema_based_automerge: bool = False,
        segments: Optional[Dict[str, SegmentationPartition]] = None,
        validators: Optional[Dict[str, List[Validator]]] = None,
        udf_specs: Optional[List[UdfSpec]] = None,
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
        self.name_udfs: Dict[str, Dict[str, Callable]] = defaultdict(dict)
        self.type_udfs: Dict[str, Dict[str, Callable]] = defaultdict(dict)
        self.multicolumn_udfs: List[UdfSpec] = []
        udf_specs = udf_specs if udf_specs else []
        for spec in udf_specs:
            if isinstance(spec.column_name, list):
                self.multicolumn_udfs.append(spec)
            elif spec.column_name:
                self.name_udfs[spec.column_name].update(spec.udfs)
            else:
                self.type_udfs[spec.column_type.__name__].update(spec.udfs)  # type: ignore

    def copy(self) -> DatasetSchema:
        copy = super().copy()
        copy.name_udf_specs = self.name_udfs.copy()
        copy.type_udf_specs = self.name_udfs.copy()
        return copy

    def _run_udfs_on_row(self, row: Mapping[str, Any], new_columns: Dict[str, Any]) -> None:
        for spec in self.multicolumn_udfs:
            if spec.column_name and set(spec.column_name).issubset(set(row.keys())):
                _apply_udfs_on_dataframe(row, None, spec.udfs, new_columns)

        for column, value in row.items():
            why_type = type(self.type_mapper(type(value))).__name__
            if column in self.name_udfs:
                _apply_udfs_on_row(value, column, self.name_udfs[column], new_columns)
            elif why_type in self.type_udfs:
                _apply_udfs_on_row(value, column, self.type_udfs[why_type], new_columns)

    def _run_udfs_on_dataframe(self, pandas: pd.DataFrame, new_df: pd.DataFrame) -> None:
        for spec in self.multicolumn_udfs:
            if spec.column_name and set(spec.column_name).issubset(set(pandas.keys())):
                _apply_udfs_on_dataframe(pandas[spec.column_name], None, spec.udfs, new_df)

        for column in pandas.keys():
            why_type = type(self.type_mapper(pandas.dtypes[column])).__name__
            if column in self.name_udfs:
                _apply_udfs_on_dataframe(pandas, column, self.name_udfs[column], new_df)
            elif why_type in self.type_udfs:
                _apply_udfs_on_dataframe(pandas, column, self.type_udfs[why_type], new_df)

    def _run_udfs(
        self, pandas: Optional[pd.DataFrame] = None, row: Optional[Dict[str, Any]] = None
    ) -> Tuple[Optional[pd.DataFrame], Optional[Mapping[str, Any]]]:
        new_columns = deepcopy(row) if row else None
        new_df = pd.DataFrame(pandas) if pandas is not None else None
        if row is not None:
            self._run_udfs_on_row(row, new_columns)  # type: ignore

        if pandas is not None:
            self._run_udfs_on_dataframe(pandas, new_df)

        return new_df, new_columns


_col_name_udfs: Dict[str, Dict[str, Callable[[Any], Any]]] = defaultdict(dict)
_col_type_udfs: Dict[str, Dict[str, Callable[[Any], Any]]] = defaultdict(dict)
_multicolumn_udfs: List[UdfSpec] = []
_resolver_specs: List[ResolverSpec] = []


def register_dataset_udf(
    col_name: Optional[Union[str, List[str]]] = None,
    col_type: Optional[DataType] = None,
    udf_name: Optional[str] = None,
    metrics: Optional[List[MetricSpec]] = None,
) -> Callable[[Any], Any]:
    """
    Decorator to easily configure UDFs for your data set. Decorate your UDF
    functions, then call generate_udf_dataset_schema() to create a UdfSchema
    that includes the UDFs configured by your decorator parameters.

    You must specify exactly one of either col_name or col_type. col_name will attach
    a UDF to the named input column(s). col_type will attach a UDF to all
    input columns of the specified type. The decorated function will automatically
    be a UDF in the UdfSchema.

    Specify udf_name to give the output of the UDF a name. udf_name
    defautls to the name of the decorated function. Note that all lambdas are
    named "lambda", so omitting udf_name on more than one lambda will result
    in name collisions.
    """

    def decorator_register(func):
        global _col_name_udfs, _col_type_udfs, _metric_specs, _multicolumn_udfs

        if col_name is not None and col_type is not None:
            raise ValueError("Only specify one of column name or type")

        if col_name is None and col_type is None:
            raise ValueError("Must specify one of column name or type")

        if metrics and col_type:
            raise ValueError("You cannot specify both metrics and column type")

        name = udf_name or func.__name__
        if isinstance(col_name, list):
            _multicolumn_udfs.append(UdfSpec(col_name, None, {name: func}))
        elif col_name is not None:
            _col_name_udfs[col_name][name] = func
        else:
            _col_type_udfs[col_type.__name__][name] = func

        if metrics:
            output_name = name if isinstance(col_name, list) else f"{col_name}.{name}"
            _resolver_specs.append(ResolverSpec(output_name, None, deepcopy(metrics)))

        return func

    return decorator_register


def generate_udf_specs(other_udf_specs: Optional[List[UdfSpec]] = None) -> List[UdfSpec]:
    """
    Generates a list UdfSpecs that implement the UDFs specified
    by the @register_dataset_udf decorators. You can provide a list of
    other_udf_specs to include in addition to those UDFs registered via
    the decorator.

    For example:

    @register_dataset_udf(col_name="col1")
    def add5(x):
        return x + 5

    @register_dataset_udf(col_type=String)
    def upper(x):
        return x.upper()

    schema = UdfSchema(STANDARD_RESOLVER, udf_specs=generate_udf_specs())
    why.log(data, schema=schema)

    This will attach a UDF to column "col1" that will generate a new column
    named "col1.add5" containing the values in "col1" incremented by 5, and a UdfMetric
    for each string column that will include a submetric named "<source>.upper" tracking
    the uppercased strings in the input columns. Since these are appended to the
    STANDARD_RESOLVER, the default metrics are also tracked for every column.
    """
    specs = list(other_udf_specs) if other_udf_specs else []
    specs += _multicolumn_udfs
    for col_name, udfs in _col_name_udfs.items():
        specs.append(UdfSpec(col_name, None, udfs))

    for col_type, udfs in _col_type_udfs.items():
        specs.append(UdfSpec(None, getattr(sys.modules["whylogs.core.datatypes"], col_type), udfs))

    return specs


def generate_udf_dataset_schema(
    other_udf_specs: Optional[List[UdfSpec]] = None,
    resolvers: Optional[List[ResolverSpec]] = None,
    types: Optional[Dict[str, Any]] = None,
    default_config: Optional[MetricConfig] = None,
    type_mapper: Optional[TypeMapper] = None,
    cache_size: int = 1024,
    schema_based_automerge: bool = False,
    segments: Optional[Dict[str, SegmentationPartition]] = None,
    validators: Optional[Dict[str, List[Validator]]] = None,
) -> UdfSchema:
    """
    Returns a UdfSchema that implements any registered UDFs, along with any
    other_udf_specs or resovlers passed in.
    """

    resolver_specs = resolvers + _resolver_specs if resolvers else STANDARD_RESOLVER + _resolver_specs
    return UdfSchema(
        resolver_specs,
        types,
        default_config,
        type_mapper,
        cache_size,
        schema_based_automerge,
        segments,
        validators,
        generate_udf_specs(other_udf_specs),
    )
