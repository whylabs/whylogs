import logging
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type, TypeVar

from typing_extensions import TypeAlias

from whylogs.core.datatypes import (
    AnyType,
    DataType,
    Fractional,
    Integral,
    StandardTypeMapper,
    String,
    TypeMapper,
)
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.metrics import Metric, MetricConfig

logger = logging.getLogger(__name__)


M = TypeVar("M", bound=Metric)
ColumnSchema: TypeAlias = "ColumnSchema"  # type: ignore


class Resolver(ABC):
    """A resolver maps from a column name and a data type to trackers.

    Note that the key of the result dictionaries defines the namespaces of the metrics in the serialized form."""

    @abstractmethod
    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        raise NotImplementedError


class StandardResolver(Resolver):
    """Standard metric resolution with builtin types."""

    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        metrics: List[StandardMetric] = [StandardMetric.counts, StandardMetric.types]

        if isinstance(why_type, Integral):
            metrics.append(StandardMetric.distribution)
            metrics.append(StandardMetric.ints)
            metrics.append(StandardMetric.cardinality)
            if not column_schema.cfg.fi_disabled:
                metrics.append(StandardMetric.frequent_items)
        elif isinstance(why_type, Fractional):
            metrics.append(StandardMetric.cardinality)
            metrics.append(StandardMetric.distribution)
        elif isinstance(why_type, String):  # Catch all category as we map 'object' here
            metrics.append(StandardMetric.cardinality)
            if column_schema.cfg.track_unicode_ranges:
                metrics.append(StandardMetric.unicode_range)
            metrics.append(StandardMetric.distribution)  # 'object' columns can contain Decimal
            if not column_schema.cfg.fi_disabled:
                metrics.append(StandardMetric.frequent_items)

        result: Dict[str, Metric] = {}
        for m in metrics:
            result[m.name] = m.zero(column_schema.cfg)
        return result


class LimitedTrackingResolver(Resolver):
    """Resolver that skips frequent item and cardinality trackers."""

    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        metrics: List[StandardMetric] = [StandardMetric.counts, StandardMetric.types]
        if isinstance(why_type, Integral):
            metrics.append(StandardMetric.distribution)
            metrics.append(StandardMetric.ints)

        elif isinstance(why_type, Fractional):
            metrics.append(StandardMetric.distribution)

        result: Dict[str, Metric] = {}
        for m in metrics:
            result[m.name] = m.zero(column_schema.cfg)
        return result


class HistogramCountingTrackingResolver(Resolver):
    """Resolver that only adds distribution tracker."""

    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        metrics: List[StandardMetric] = [StandardMetric.distribution]
        result: Dict[str, Metric] = {}
        for m in metrics:
            result[m.name] = m.zero(column_schema.cfg)
        return result


@dataclass
class MetricSpec:
    """
    Specify a Metric to instantiate.
    """

    metric: Any  # Should be a subclass of Metric, it should be the class, not an instance
    config: Optional[MetricConfig] = None  # omit to use default MetricConfig

    def __post_init__(self):
        if not issubclass(self.metric, Metric):
            raise ValueError("MetricSpec: must supply a Metric subclass to MetricSpec")


@dataclass
class ResolverSpec:
    """
    Specify the metrics to instantiate for matching columns. column_name
    takes precedence over column_type. column_type should be a subclass
    of DataType, i.e., AnyType, Frational, Integral, or String. Pass the
    class, not an instance. If the exclude field is True, the listed metrics
    will be excluded from any matched columns.
    """

    column_name: Optional[str] = None  # TODO: maybe make this a regex
    column_type: Optional[Type] = None
    metrics: List[MetricSpec] = field(default_factory=list)
    exclude: bool = False
    type_mapper: TypeMapper = field(default_factory=StandardTypeMapper)

    def __post_init__(self):
        if self.column_name and self.column_type:
            logger.warning(f"ResolverSpec: column {self.column_name} also specified type, name takes precedence")
        if not (self.column_name or self.column_type):
            raise ValueError("ResolverSpec: resolver specification must supply name or type")

        try:
            if self.column_type and not issubclass(self.column_type, DataType):
                self.column_type = type(self.type_mapper(self.column_type))
        except:  # noqa
            raise ValueError("ResolverSpec: resolver specification column type must be a DataType")


# whylabs expects COLUMN_METRICS to be present for every column.
COLUMN_METRICS = [MetricSpec(StandardMetric.counts.value), MetricSpec(StandardMetric.types.value)]


class DeclarativeResolverBase(Resolver):
    """
    Implements the declarative resolution logic by interpreting a "program"
    of ResolverSpecs
    """

    def __init__(self, resolvers: List[ResolverSpec], default_config: Optional[MetricConfig] = None) -> None:
        self._resolvers = [deepcopy(r) for r in resolvers if not r.exclude]
        self._anti_resolvers = [deepcopy(ar) for ar in resolvers if ar.exclude]
        self._default_config = default_config

    def _allowed_metric(self, name: str, why_type: DataType, config: MetricConfig, metric: Metric) -> bool:
        """Return False for any metrics turned off in the config"""

        namespace = metric.get_namespace()
        if config.fi_disabled and namespace == "frequent_items":
            return False
        if (not config.track_unicode_ranges) and namespace == "unicode_range":
            return False
        for ar in self._anti_resolvers:
            if name == ar.column_name or (ar.column_name is None and isinstance(why_type, ar.column_type)):  # type: ignore
                if metric in [m.metric for m in ar.metrics]:
                    return False

        return True

    def _resolve(self, name: str, why_type: DataType, config: Optional[MetricConfig]) -> Dict[str, Metric]:
        result: Dict[str, Metric] = {}
        for resolver_spec in self._resolvers:
            col_name, col_type = resolver_spec.column_name, resolver_spec.column_type
            if col_name == name or (col_name is None and isinstance(why_type, col_type)):  # type: ignore
                for spec in resolver_spec.metrics:
                    cfg = spec.config or self._default_config or config or MetricConfig()
                    if self._allowed_metric(name, why_type, cfg, spec.metric):
                        if spec.metric.get_namespace() in result:
                            logger.warning(
                                f"Conflicting resolvers for {spec.metric.get_namespace()} metric in column '{name}' of type {why_type.return_type.__name__}"
                            )
                        result[spec.metric.get_namespace()] = spec.metric.zero(cfg)

        return result


# STANDARD_RESOLVER matches the default DatasetSchema/StandardResolver behavior
STANDARD_RESOLVER = [
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

NO_FI_RESOLVER = [
    ResolverSpec(
        column_type=Integral,
        metrics=COLUMN_METRICS
        + [
            MetricSpec(StandardMetric.distribution.value),
            MetricSpec(StandardMetric.ints.value),
            MetricSpec(StandardMetric.cardinality.value),
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
        ],
    ),
    ResolverSpec(column_type=AnyType, metrics=COLUMN_METRICS),
]

LIMITED_TRACKING_RESOLVER = [
    ResolverSpec(
        column_type=Integral,
        metrics=COLUMN_METRICS
        + [
            MetricSpec(StandardMetric.distribution.value),
            MetricSpec(StandardMetric.ints.value),
        ],
    ),
    ResolverSpec(
        column_type=Fractional,
        metrics=COLUMN_METRICS
        + [
            MetricSpec(StandardMetric.distribution.value),
        ],
    ),
    ResolverSpec(column_type=String, metrics=COLUMN_METRICS),
    ResolverSpec(column_type=AnyType, metrics=COLUMN_METRICS),
]


HISTOGRAM_COUNTING_TRACKING_RESOLVER = [
    ResolverSpec(column_type=Integral, metrics=[MetricSpec(StandardMetric.distribution.value)]),
    ResolverSpec(column_type=Fractional, metrics=[MetricSpec(StandardMetric.distribution.value)]),
    ResolverSpec(column_type=String, metrics=[MetricSpec(StandardMetric.distribution.value)]),
    ResolverSpec(column_type=AnyType, metrics=[MetricSpec(StandardMetric.distribution.value)]),
]


DEFAULT_RESOLVER: List[ResolverSpec] = list(STANDARD_RESOLVER)


class DeclarativeResolver(DeclarativeResolverBase):
    """
    Implements the declarative resolution logic by interpreting a "program"
    of ResolverSpecs
    """

    def __init__(
        self, resolvers: Optional[List[ResolverSpec]] = None, default_config: Optional[MetricConfig] = None
    ) -> None:
        if resolvers is None:
            resolvers = DEFAULT_RESOLVER

        super().__init__(resolvers, default_config)

    def add_resolver(self, resolver_spec: ResolverSpec):
        if not resolver_spec.exclude:
            self._resolvers.append(deepcopy(resolver_spec))
        else:
            self._anti_resolvers.append(deepcopy(resolver_spec))

    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        return self._resolve(name, why_type, column_schema.cfg)
