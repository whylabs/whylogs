import logging
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Callable, Dict, List, Optional, Tuple

from whylogs.core.datatypes import DataType, StandardTypeMapper, TypeMapper
from whylogs.core.metrics.metrics import (
    Metric,
    MetricConfig,
    OperationResult,
    register_metric,
)
from whylogs.core.metrics.multimetric import MultiMetric, SubmetricSchema
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.resolvers import (
    STANDARD_RESOLVER,
    UDF_BASE_RESOLVER,
    MetricSpec,
    ResolverSpec,
    _allowed_metric,
)
from whylogs.core.schema import DeclarativeSchema
from whylogs.core.segmentation_partition import SegmentationPartition
from whylogs.core.utils import deprecated
from whylogs.core.validators.validator import Validator

logger = logging.getLogger(__name__)


class DeclarativeSubmetricSchema(SubmetricSchema):
    """
    The DeclarativeSubmetricSchema allows one to customize the set of metrics
    tracked for each UDF computed by a UdfMetric. Pass its constructor a list
    of ResolverSpecs, which specify the UDF name or data type to
    match and the list of MetricSpecs to instantiate for matching UDFs.
    Each MetricSpec specifies the Metric class and MetricConfig to
    instantiate. Omit the MetricSpec::config to use the default MetricConfig.

    For example, DeclarativeSubmetricSchema(resolvers=STANDARD_RESOLVER) implements
    the same schema as DatasetSchema(), i.e., using the default MetricConfig,
    StandardTypeMapper, StandardResolver, etc.  STANDARD_RESOLVER is defined
    in whylogs/python/whylogs/core/resolvers.py
    """

    def __init__(self, resolvers: List[ResolverSpec], default_config: Optional[MetricConfig] = None) -> None:
        self._default_config = default_config or MetricConfig()
        self._resolvers = resolvers.copy()

    def resolve(self, name: str, why_type: DataType, fi_disabled: bool = False) -> Dict[str, Metric]:
        result: Dict[str, Metric] = {}
        for resolver_spec in self._resolvers:
            col_name, col_type = resolver_spec.column_name, resolver_spec.column_type
            if (col_name and col_name == name) or (col_name is None and isinstance(why_type, col_type)):  # type: ignore
                for spec in resolver_spec.metrics:
                    config = spec.config or self._default_config
                    if _allowed_metric(config, spec.metric):
                        result[spec.metric.get_namespace()] = spec.metric.zero(config)

        return result


def default_schema() -> DeclarativeSubmetricSchema:
    return DeclarativeSubmetricSchema(STANDARD_RESOLVER)


@dataclass(frozen=True)
class UdfMetricConfig(MetricConfig):
    """
    Configure UDFs & submetrics for UdfMetric

    Attributes:
       udfs: Maps submetric name to the UDF that computes the value to track

       submetric_schema [optional]: determines the set of metrics tracked for
           each computed value

       type_mapper [optional]: maps Python types to whylogs DataType
    """

    udfs: Dict[str, Callable[[Any], Any]] = field(default_factory=dict)
    submetric_schema: SubmetricSchema = field(default_factory=default_schema)
    type_mapper: TypeMapper = field(default_factory=StandardTypeMapper)


class UdfMetric(MultiMetric):
    """
    Applies the specified UDFs to the input column values and tracks the metrics
    specified by the submetric_schema to their output.

    Args:
        udfs: map of submetric name to UDF to compute value to track

        submetric_schema [optional]: determines the set of metrics to track for
            each UDF output. Defaults to STANDARD_RESOLVER

        type_mapper [optional]: maps Python types to whylogs DataType. Defaults
            to StandardTypeMapper

        fi_disabled [optional]: Should FrequentItemsMetric tracking be disabled.
            Defaults to False
    """

    def __init__(
        self,
        udfs: Dict[str, Callable[[Any], Any]],
        # discover these with resolver  submetrics: Dict[str, Dict[str, Metric]],  # feature name -> (namespace -> metric)
        submetric_schema: Optional[SubmetricSchema] = None,
        type_mapper: Optional[TypeMapper] = None,
        fi_disabled: bool = False,
    ):
        super().__init__(dict())  # submetrics)
        self._udfs = udfs
        self._submetric_schema = submetric_schema or default_schema()
        self._type_mapper = type_mapper or StandardTypeMapper()
        self._fi_disabled = fi_disabled

    @property
    def namespace(self) -> str:
        return "udf"

    def merge(self, other: "UdfMetric") -> "UdfMetric":
        merged = UdfMetric(self._udfs, self._submetric_schema, self._type_mapper, self._fi_disabled)
        merged.submetrics = self.merge_submetrics(other)
        return merged

    def _add_submetric(self, name: str, value: Any) -> None:
        self.submetrics[name] = self._submetric_schema.resolve(name, self._type_mapper(type(value)), self._fi_disabled)

    def _update_relevant_submetrics(self, name: str, data: PreprocessedColumn) -> None:
        if name not in self.submetrics:
            return

        for metric in self.submetrics[name].values():
            metric.columnar_update(data)

    def columnar_update(self, view: PreprocessedColumn) -> OperationResult:
        successes = 0
        failures = 0
        for value in list(chain.from_iterable(view.raw_iterator())):
            ok = True
            for submetric_name, udf in self._udfs.items():
                try:
                    computed_value = udf(value)
                    if submetric_name not in self.submetrics:
                        self._add_submetric(submetric_name, computed_value)  # NOTE: assumes column is homogeneous-ish?

                    data = PreprocessedColumn._process_scalar_value(computed_value)
                    self._update_relevant_submetrics(submetric_name, data)
                except Exception:  # noqa
                    logger.exception(f"UDF {submetric_name} evaluation failed")
                    ok = False

            if ok:
                successes += 1
            else:
                failures += 1

        return OperationResult(failures, successes)

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "UdfMetric":
        config = config or UdfMetricConfig()
        if not isinstance(config, UdfMetricConfig):
            logger.error("UdfMetric.zero() needs an UdfMetricConfig")
            config = UdfMetricConfig()

        return UdfMetric(
            config.udfs,
            config.submetric_schema,
            config.type_mapper,
            config.fi_disabled,
        )


# Register it so Multimetric and ProfileView can deserialize
register_metric(UdfMetric)


_col_name_submetrics: Dict[str, List[Tuple[str, Callable[[Any], Any]]]] = defaultdict(list)
_col_name_submetric_schema: Dict[str, SubmetricSchema] = dict()
_col_name_type_mapper: Dict[str, TypeMapper] = dict()

_col_type_submetrics: Dict[DataType, List[Tuple[str, Callable[[Any], Any]]]] = defaultdict(list)
_col_type_submetric_schema: Dict[DataType, SubmetricSchema] = dict()
_col_type_type_mapper: Dict[DataType, TypeMapper] = dict()


def register_metric_udf(
    col_name: Optional[str] = None,
    col_type: Optional[DataType] = None,
    submetric_name: Optional[str] = None,
    submetric_schema: Optional[SubmetricSchema] = None,
    type_mapper: Optional[TypeMapper] = None,
) -> Callable[[Any], Any]:
    """
    Decorator to easily configure UdfMetrics for your data set. Decorate your UDF
    functions, then call generate_udf_schema() to generate a list of ResolverSpecs
    that include the UdfMetrics configured by your decorator parameters.

    You must specify exactly one of either col_name or col_type. col_name will attach
    a UdfMetric to the named input column. col_type will attach a UdfMetric to all
    input columns of the specified type. The decorated function will automatically
    be a UDF in the UdfMetric.

    Specify submetric_name to give the output of the UDF a name. submetric_name
    defautls to the name of the decorated function. Note that all lambdas are
    named "lambda" so omitting submetric_name on more than one lambda will result
    in name collisions.

    You can optionally pass submetric_schema to specify and configure the metrics
    to be tracked for each UDF. This defualts to the STANDARD_RESOLVER metrics.

    You can optionally pass type_mapper to control how Python types are mapped to
    whylogs DataTypes. This defaults to the StandardTypeMapper.
    """

    def decorator_register(func):
        global _col_name_submetrics, _col_name_submetric_schema, _col_name_type_mapper
        global _col_type_submetrics, _col_type_submetric_schema, _col_type_type_mapper

        if col_name is not None and col_type is not None:
            raise ValueError("Only specify one of column name or type")

        if col_name is None and col_type is None:
            raise ValueError("Must specify one of column name or type")

        subname = submetric_name or func.__name__
        if col_name is not None:
            _col_name_submetrics[col_name].append((subname, func))
            if submetric_schema is not None:
                if col_name in _col_name_submetric_schema:
                    logger.warn(f"Overwriting submetric schema for column {col_name}")
                _col_name_submetric_schema[col_name] = submetric_schema
            if type_mapper is not None:
                if col_name in _col_name_type_mapper:
                    logger.warn(f"Overwriting UdfMetric type mapper for column {col_name}")
                _col_name_type_mapper[col_name] = type_mapper
        else:
            _col_type_submetrics[col_type].append((subname, func))
            if submetric_schema is not None:
                if col_type in _col_type_submetric_schema:
                    logger.warn(f"Overwriting submetric schema for column type {col_type}")
                _col_type_submetric_schema[col_type] = submetric_schema
            if type_mapper is not None:
                if col_type in _col_type_type_mapper:
                    logger.warn(f"Overwriting UdfMetric type mapper for column type {col_type}")
                _col_type_type_mapper[col_type] = type_mapper

        return func

    return decorator_register


def generate_udf_resolvers() -> List[ResolverSpec]:
    """
    Generates a list of ResolverSpecs that implement the UdfMetrics specified
    by the @register_metric_udf decorators. The result only includes the UdfMetric,
    so you may want to append it to a list of ResolverSpecs defining the other
    metrics you wish to track.

    For example:

    @register_metric_udf(col_name="col1")
    def add5(x):
        return x + 5

    @register_metric_udf(col_type=String)
    def upper(x):
        return x.upper()

    schema = DeclarativeSchema(STANDARD_RESOLVER + generate_udf_schema())
    why.log(data, schema=schema)

    This will attach a UdfMetric to column "col1" that will include a submetric
    named "add5" tracking the values in "col1" incremented by 5, and a UdfMetric
    for each string column that will include a submetric named "upper" tracking
    the uppercased strings in the input columns. Since these are appended to the
    STANDARD_RESOLVER, the default metrics are also tracked for every column.
    """

    resolvers: List[ResolverSpec] = list()
    udfs: Dict[str, Callable[[Any], Any]]
    for col_name, submetrics in _col_name_submetrics.items():
        udfs = dict()
        for submetric in submetrics:
            udfs[submetric[0]] = submetric[1]
        config = UdfMetricConfig(
            udfs=udfs,
            submetric_schema=_col_name_submetric_schema.get(col_name) or default_schema(),
            type_mapper=_col_name_type_mapper.get(col_name) or StandardTypeMapper(),
        )
        resolvers.append(ResolverSpec(col_name, None, [MetricSpec(UdfMetric, config)]))

    for col_type, submetrics in _col_type_submetrics.items():
        udfs = dict()
        for submetric in submetrics:
            udfs[submetric[0]] = submetric[1]
        config = UdfMetricConfig(
            udfs=udfs,
            submetric_schema=_col_type_submetric_schema.get(col_type) or default_schema(),
            type_mapper=_col_type_type_mapper.get(col_type) or StandardTypeMapper(),
        )
        resolvers.append(ResolverSpec(None, col_type, [MetricSpec(UdfMetric, config)]))

    return resolvers


@deprecated(message="Please use generate_udf_resovlers()")
def generate_udf_schema() -> List[ResolverSpec]:
    return generate_udf_resolvers()


def udf_metric_schema(
    non_udf_resolvers: Optional[List[ResolverSpec]] = None,
    types: Optional[Dict[str, Any]] = None,
    default_config: Optional[MetricConfig] = None,
    type_mapper: Optional[TypeMapper] = None,
    cache_size: int = 1024,
    schema_based_automerge: bool = False,
    segments: Optional[Dict[str, SegmentationPartition]] = None,
    validators: Optional[Dict[str, List[Validator]]] = None,
) -> DeclarativeSchema:
    """
    Generates a DeclarativeSchema that implement the UdfMetrics specified
    by the @register_metric_udf decorators (in additon to any non_udf_resolvers
    passed in).

    For example:

    @register_metric_udf(col_name="col1")
    def add5(x):
        return x + 5

    @register_metric_udf(col_type=String)
    def upper(x):
        return x.upper()

    why.log(data, schema=udf_metric_schema())

    This will attach a UdfMetric to column "col1" that will include a submetric
    named "add5" tracking the values in "col1" incremented by 5, and a UdfMetric
    for each string column that will include a submetric named "upper" tracking
    the uppercased strings in the input columns. Since these are appended to the
    STANDARD_RESOLVER, the default metrics are also tracked for every column.
    """

    resolvers = generate_udf_resolvers()
    non_udf_resolvers = non_udf_resolvers if non_udf_resolvers is not None else UDF_BASE_RESOLVER

    return DeclarativeSchema(
        non_udf_resolvers + resolvers,
        types,
        default_config,
        type_mapper,
        cache_size,
        schema_based_automerge,
        segments,
        validators,
    )
