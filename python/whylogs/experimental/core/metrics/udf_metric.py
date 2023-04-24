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
    MetricSpec,
    ResolverSpec,
    _allowed_metric,
)

logger = logging.getLogger(__name__)


class DeclarativeSubmetricSchema(SubmetricSchema):
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


DefaultSchema = DeclarativeSubmetricSchema(STANDARD_RESOLVER)


@dataclass(frozen=True)
class UdfMetricConfig(MetricConfig):
    """Maps feature name to callable that computes it"""

    udfs: Dict[str, Callable[[Any], Any]] = field(default_factory=dict)
    submetric_schema: SubmetricSchema = field(default_factory=lambda: DefaultSchema)
    type_mapper: TypeMapper = field(default_factory=StandardTypeMapper)


class UdfMetric(MultiMetric):
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
        self._submetric_schema = submetric_schema or DefaultSchema()
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
        count = 0
        for value in list(chain.from_iterable(view.raw_iterator())):
            for submetric_name, udf in self._udfs.items():
                computed_value = udf(value)
                if submetric_name not in self.submetrics:
                    self._add_submetric(submetric_name, computed_value)  # NOTE: assumes column is homogeneous-ish?

                data = PreprocessedColumn._process_scalar_value(computed_value)
                self._update_relevant_submetrics(submetric_name, data)

            count += 1
        return OperationResult.ok(count)

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
    def decorator_register(func):
        global _col_name_submetrics, _col_name_submetric_schema, _col_name_type_mapper

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


def generate_udf_schema() -> List[ResolverSpec]:
    resolvers: List[ResolverSpec] = list()
    udfs: Dict[str, Callable[[Any], Any]]
    for col_name, submetrics in _col_name_submetrics.items():
        udfs = dict()
        for submetric in submetrics:
            udfs[submetric[0]] = submetric[1]
        config = UdfMetricConfig(
            udfs=udfs,
            submetric_schema=_col_name_submetric_schema.get(col_name) or DefaultSchema,
            type_mapper=_col_name_type_mapper.get(col_name) or StandardTypeMapper(),
        )
        resolvers.append(ResolverSpec(col_name, None, [MetricSpec(UdfMetric, config)]))

    for col_type, submetrics in _col_type_submetrics.items():
        udfs = dict()
        for submetric in submetrics:
            udfs[submetric[0]] = submetric[1]
        config = UdfMetricConfig(
            udfs=udfs,
            submetric_schema=_col_type_submetric_schema.get(col_type) or DefaultSchema,
            type_mapper=_col_type_type_mapper.get(col_type) or StandardTypeMapper(),
        )
        resolvers.append(ResolverSpec(None, col_type, [MetricSpec(UdfMetric, config)]))

    return resolvers
