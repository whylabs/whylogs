import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TypeVar

from typing_extensions import TypeAlias

from whylogs.core.datatypes import DataType, Fractional, Integral, String
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.metrics import Metric, MetricConfig

logger = logging.getLogger(__name__)


M = TypeVar("M", bound=Metric)
ColumnSchema: TypeAlias = "ColumnSchema"  # type: ignore


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


class Resolver(ABC):
    """A resolver maps from a column name and a data type to trackers.

    Note that the key of the result dictionaries defines the namespaces of the metrics in the serialized form."""

    def __init__(self):
        self.additional_specs: List[ResolverSpec] = []

    def add_resolver_spec(self, resolver_spec: ResolverSpec):
        """Add resolver specification to existing resolver instance.

        Parameters
        ----------
        resolver_spec : ResolverSpec
            Resolver specification that define the metrics to instantiate for matching columns.
        """
        self.additional_specs.append(resolver_spec)

    @abstractmethod
    def resolve_additional_specs(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        """Resolve additional metrics from a column name or data type.

        Additional metrics are resolved using a `DeclarativeResolver` by passing a list of `ResolverSpec` objects obtained from `add_resolver_spec`.
        The resolved metrics are returned as a dictionary mapping metric names to `Metric` objects.
        """
        additional_result: Dict[str, Metric] = {}
        if self.additional_specs:
            additional_result = DeclarativeResolver(self.additional_specs).resolve(
                name=name, why_type=why_type, column_schema=column_schema
            )
        return additional_result

    @abstractmethod
    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        raise NotImplementedError


class StandardResolver(Resolver):
    """Standard metric resolution with builtin types."""

    def resolve_additional_specs(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        return super().resolve_additional_specs(name=name, why_type=why_type, column_schema=column_schema)

    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:

        metrics: List[StandardMetric] = [StandardMetric.counts, StandardMetric.types]

        if isinstance(why_type, Integral):
            metrics.append(StandardMetric.distribution)
            metrics.append(StandardMetric.ints)
            metrics.append(StandardMetric.cardinality)
            metrics.append(StandardMetric.frequent_items)
        elif isinstance(why_type, Fractional):
            metrics.append(StandardMetric.cardinality)
            metrics.append(StandardMetric.distribution)
        elif isinstance(why_type, String):  # Catch all category as we map 'object' here
            metrics.append(StandardMetric.cardinality)
            if column_schema.cfg.track_unicode_ranges:
                metrics.append(StandardMetric.unicode_range)
            metrics.append(StandardMetric.distribution)  # 'object' columns can contain Decimal
            metrics.append(StandardMetric.frequent_items)

        if column_schema.cfg.fi_disabled:
            metrics.remove(StandardMetric.frequent_items)

        result: Dict[str, Metric] = {}
        for m in metrics:
            result[m.name] = m.zero(column_schema.cfg)

        result.update(self.resolve_additional_specs(name=name, why_type=why_type, column_schema=column_schema))

        return result


class LimitedTrackingResolver(Resolver):
    """Resolver that skips frequent item and cardinality trackers."""

    def resolve_additional_specs(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        return super().resolve_additional_specs(name=name, why_type=why_type, column_schema=column_schema)

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

        result.update(self.resolve_additional_specs(name=name, why_type=why_type, column_schema=column_schema))

        return result


class HistogramCountingTrackingResolver(Resolver):
    """Resolver that only adds distribution tracker."""

    def resolve_additional_specs(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        return super().resolve_additional_specs(name=name, why_type=why_type, column_schema=column_schema)

    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        metrics: List[StandardMetric] = [StandardMetric.distribution]
        result: Dict[str, Metric] = {}
        for m in metrics:
            result[m.name] = m.zero(column_schema.cfg)

        result.update(self.resolve_additional_specs(name=name, why_type=why_type, column_schema=column_schema))

        return result


class EmptyResolver(Resolver):
    """Resolver that has no trackers. Meant to be used by calling add_resolver_spec once it's instantiated."""

    def resolve_additional_specs(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        return super().resolve_additional_specs(name=name, why_type=why_type, column_schema=column_schema)

    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        result: Dict[str, Metric] = {}
        result.update(self.resolve_additional_specs(name=name, why_type=why_type, column_schema=column_schema))
        return result


# whylabs expects COLUMN_METRICS to be present for every column.
COLUMN_METRICS = [MetricSpec(StandardMetric.counts.value), MetricSpec(StandardMetric.types.value)]


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

    def resolve_additional_specs(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        pass

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
