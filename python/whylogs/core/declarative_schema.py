import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from whylogs.core.datatypes import (
    AnyType,
    DataType,
    Fractional,
    Integral,
    String,
    TypeMapper,
)
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.metrics import Metric, MetricConfig
from whylogs.core.resolvers import Resolver
from whylogs.core.schema import ColumnSchema, DatasetSchema
from whylogs.core.segmentation_partition import SegmentationPartition

logger = logging.getLogger(__name__)


@dataclass
class MetricSpec:
    metric: Any  # Should be <: Metric
    config: Optional[MetricConfig] = None


@dataclass
class ResolverSpec:
    column_name: Optional[str] = None  # TODO: maybe make this a regex
    column_type: Optional[Any] = None
    metrics: List[MetricSpec] = field(default_factory=list)


COLUMN_METRICS = [MetricSpec(StandardMetric.counts.value), MetricSpec(StandardMetric.types.value)]

STANDARD_RESOLVER = [  # TODO: maybe move this to unit test?
    ResolverSpec(
        column_type=Integral,
        metrics=COLUMN_METRICS
        + [
            MetricSpec(StandardMetric.distribution.value),
            MetricSpec(StandardMetric.ints.value),
            MetricSpec(StandardMetric.cardinality.value),
            MetricSpec(StandardMetric.frequent_items.value),
        ],
    ),
    ResolverSpec(
        column_type=Fractional,
        metrics=COLUMN_METRICS
        + [
            MetricSpec(StandardMetric.distribution.value),
            MetricSpec(StandardMetric.cardinality.value),
        ],
    ),
    ResolverSpec(
        column_type=String,
        metrics=COLUMN_METRICS
        + [
            MetricSpec(StandardMetric.unicode_range.value),
            MetricSpec(StandardMetric.distribution.value),
            MetricSpec(StandardMetric.cardinality.value),
            MetricSpec(StandardMetric.frequent_items.value),
        ],
    ),
    ResolverSpec(column_type=AnyType, metrics=COLUMN_METRICS),
]


def _allowed_metric(config: MetricConfig, metric: Metric) -> bool:
    namespace = metric.get_namespace()
    if config.fi_disabled and namespace == "frequent_items":
        return False
    if (not config.track_unicode_ranges) and namespace == "unicode_range":
        return False
    return True


class DeclarativeResolver(Resolver):
    def __init__(self, resolvers: List[ResolverSpec], default_config: Optional[MetricConfig] = None) -> None:
        # Validate resolvers -- must have name xor type, MetricSpec metrcis must <: Metric
        for spec in resolvers:
            if spec.column_name and spec.column_type:
                logger.warning(
                    f"DeclarativeSchema: column {spec.column_name} also specified type, name takes precedence"
                )
            if not (spec.column_name or spec.column_type):
                raise ValueError("DeclarativeSchema: resolver specification must supply name or type")

            if spec.column_type and not issubclass(spec.column_type, DataType):
                raise ValueError("DeclarativeSchema: resolver specification column type must be a DataType")

            for metric_spec in spec.metrics:
                if not issubclass(metric_spec.metric, Metric):
                    raise ValueError("DeclarativeSchema: must supply a Metric subclass to MetricSpec")

        self._resolvers = resolvers

    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        result: Dict[str, Metric] = {}
        for resolver_spec in self._resolvers:
            col_name, col_type = resolver_spec.column_name, resolver_spec.column_type
            if (col_name and col_name == name) or (col_name is None and isinstance(why_type, col_type)):  # type: ignore
                for spec in resolver_spec.metrics:
                    config = spec.config or column_schema.cfg
                    if _allowed_metric(config, spec.metric):
                        result[spec.metric.get_namespace()] = spec.metric.zero(config)

        return result


class DeclarativeSchema(DatasetSchema):
    def __init__(
        self,
        resolvers: List[ResolverSpec],
        types: Optional[Dict[str, Any]] = None,
        default_config: Optional[MetricConfig] = None,
        type_mapper: Optional[TypeMapper] = None,
        cache_size: int = 1024,
        schema_based_automerge: bool = False,
        segments: Optional[Dict[str, SegmentationPartition]] = None,
    ) -> None:
        if not resolvers:
            logger.warning("No columns specified in DeclarativeSchema")
        resolver = DeclarativeResolver(resolvers, default_config)
        super().__init__(
            types=types,
            default_configs=default_config,
            type_mapper=type_mapper,
            resolvers=resolver,
            cache_size=cache_size,
            schema_based_automerge=schema_based_automerge,
            segments=segments,
        )
