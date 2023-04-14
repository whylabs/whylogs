import logging
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Callable, Dict, List, Optional

from whylogs.core.datatypes import DataType, StandardTypeMapper, TypeMapper
from whylogs.core.metrics.metrics import (
    Metric,
    MetricConfig,
    OperationResult,
    register_metric,
)
from whylogs.core.metrics.multimetric import MultiMetric, SubmetricSchema
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.resolvers import STANDARD_RESOLVER, ResolverSpec, _allowed_metric

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
