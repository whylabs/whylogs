import logging
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from whylogs.core.datatypes import TypeMapper
from whylogs.core.metrics.metrics import MetricConfig
from whylogs.core.resolvers import UDF_BASE_RESOLVER, MetricSpec, ResolverSpec
from whylogs.core.schema import DatasetSchema, DeclarativeSchema
from whylogs.core.segmentation_partition import SegmentationPartition
from whylogs.core.stubs import pd
from whylogs.core.validators.validator import Validator
from whylogs.experimental.core.metrics.udf_metric import generate_udf_resolvers

logger = logging.getLogger(__name__)


@dataclass
class UdfSpec:
    """
    Defines UDFs to apply to matching input columns. Tthe UDF is
    passed a dictionary or dataframe with the named columns available (the UDF will
    not be called unless all the named columns are available). The new column name
    the key in the udfs dictionary.
    """

    column_names: List[str]
    udfs: Dict[str, Callable[[Any], Any]] = field(
        default_factory=dict
    )  # new column name -> callable to compute new column value

    def __post_init__(self):
        if len(self.column_names) == 0 or not all([isinstance(x, str) for x in self.column_names]):
            raise ValueError("UdfSpec: column_names must be either a non-empty list of strings")


def _apply_udfs_on_row(value: Any, udfs: Dict, new_columns: Dict[str, Any]) -> None:
    for new_col, udf in udfs.items():
        try:
            new_columns[new_col] = udf(value)
        except Exception:  # noqa
            new_columns[new_col] = None
            logger.exception(f"Evaluating multi-column UDF {new_col} failed")


def _apply_udfs_on_dataframe(pandas: pd.DataFrame, udfs: Dict, new_df: pd.DataFrame) -> None:
    for new_col, udf in udfs.items():
        try:
            new_df[new_col] = udf(pandas)
        except Exception:  # noqa
            # new_df[new_col] = pandas[pandas.keys()[0]].apply(lambda x: None)
            logger.exception(f"Evaluating UDF {new_col} failed on columns {pandas.keys()}")


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
        udf_specs = udf_specs if udf_specs else []
        self.multicolumn_udfs = list(udf_specs)

    def copy(self) -> DatasetSchema:
        copy = super().copy()
        copy.multicolumn_udfs = list(self.multicolumn_udfs)
        return copy

    def _run_udfs_on_row(self, row: Mapping[str, Any], new_columns: Dict[str, Any]) -> None:
        for spec in self.multicolumn_udfs:
            if set(spec.column_names).issubset(set(row.keys())):
                _apply_udfs_on_dataframe(row, spec.udfs, new_columns)

    def _run_udfs_on_dataframe(self, pandas: pd.DataFrame, new_df: pd.DataFrame) -> None:
        for spec in self.multicolumn_udfs:
            if set(spec.column_names).issubset(set(pandas.keys())):
                _apply_udfs_on_dataframe(pandas[spec.column_names], spec.udfs, new_df)

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


_multicolumn_udfs: List[UdfSpec] = []
_resolver_specs: List[ResolverSpec] = []


def register_dataset_udf(
    col_names: List[str],
    udf_name: Optional[str] = None,
    metrics: Optional[List[MetricSpec]] = None,
) -> Callable[[Any], Any]:
    """
    Decorator to easily configure UDFs for your data set. Decorate your UDF
    functions, then call generate_udf_dataset_schema() to create a UdfSchema
    that includes the UDFs configured by your decorator parameters. The decorated
    function will automatically be a UDF in the UdfSchema.

    Specify udf_name to give the output of the UDF a name. udf_name
    defautls to the name of the decorated function. Note that all lambdas are
    named "lambda", so omitting udf_name on more than one lambda will result
    in name collisions.
    """

    def decorator_register(func):
        global _multicolumn_udfs, _resolver_specs
        name = udf_name or func.__name__
        _multicolumn_udfs.append(UdfSpec(col_names, {name: func}))
        if metrics:
            _resolver_specs.append(ResolverSpec(name, None, deepcopy(metrics)))

        return func

    return decorator_register


def generate_udf_specs(other_udf_specs: Optional[List[UdfSpec]] = None) -> List[UdfSpec]:
    """
    Generates a list UdfSpecs that implement the UDFs specified
    by the @register_dataset_udf decorators. You can provide a list of
    other_udf_specs to include in addition to those UDFs registered via
    the decorator.

    For example:

    @register_dataset_udf(col_names=["col1"])
    def add5(x):
        return x + 5

    schema = UdfSchema(STANDARD_RESOLVER, udf_specs=generate_udf_specs())
    why.log(data, schema=schema)

    This will attach a UDF to column "col1" that will generate a new column
    named "add5" containing the values in "col1" incremented by 5. Since these
    are appended to the STANDARD_RESOLVER, the default metrics are also tracked
    for every column.
    """
    specs = list(other_udf_specs) if other_udf_specs else []
    specs += _multicolumn_udfs
    return specs


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
) -> UdfSchema:
    """
    Returns a UdfSchema that implements any registered UDFs, along with any
    other_udf_specs or resovlers passed in.
    """
    if resolvers is not None:
        resolver_specs = resolvers + _resolver_specs
    else:
        resolver_specs = UDF_BASE_RESOLVER + _resolver_specs
    resolver_specs += generate_udf_resolvers()
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
