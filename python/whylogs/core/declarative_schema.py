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


"""
The DeclarativeSchema allows one to customize the set of metrics
tracked for each column in a data set. Pass its constructor a list
of ResolverSpecs, which specify the column name or data type to
match and the list of MetricSpecs to instantiate for matching columns.
Each MetricSpec specifies the Metric class and MetricConfig to
instantiate. Omit the MetricSpec::config to use the default MetricConfig.

For example, DeclarativeSchema(resolvers=STANDARD_RESOLVER) implements
the same schema as DatasetSchema(), i.e., using the default MetricConfig,
StandardTypeMapper, StandardResolver, etc.
"""


@dataclass
class MetricSpec:
    """
    Specify a Metric to instantiate.
    """

    metric: Any  # Should be a subclass of Metric, it should be the class, not an instance
    config: Optional[MetricConfig] = None  # omit to use default MetricConfig


@dataclass
class ResolverSpec:
    """
    Specify the metrics to instantiate for matching columns. column_name
    takes precedence over column_type. column_type should be a subclass
    of DataType, i.e., AnyType, Frational, Integral, or String. Pass the
    class, not an instance.
    """

    column_name: Optional[str] = None  # TODO: maybe make this a regex
    column_type: Optional[Any] = None
    metrics: List[MetricSpec] = field(default_factory=list)


# whylabs expects COLUMN_METRICS to be present for every column.
COLUMN_METRICS = [MetricSpec(StandardMetric.counts.value), MetricSpec(StandardMetric.types.value)]


# STANDARD_RESOLVER matches the default DatasetSchema/StandardResolver behavior
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
    """Return False for any metrics turned off in the config"""

    namespace = metric.get_namespace()
    if config.fi_disabled and namespace == "frequent_items":
        return False
    if (not config.track_unicode_ranges) and namespace == "unicode_range":
        return False
    return True


class DeclarativeResolver(Resolver):
    """
    Implements the declarative resolution logic by interpreting a "program"
    of ResolverSpecs
    """

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
    """
    Easily customizable DatasetSchema that implements schema defined by declarative ResolverSpec
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
