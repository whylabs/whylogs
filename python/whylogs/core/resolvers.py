from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Dict, List, TypeVar, Union

from typing_extensions import TypeAlias

from whylogs.core.datatypes import DataType, Fractional, Integral, String
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.metrics import Metric

M = TypeVar("M", bound=Metric)
ColumnSchema: TypeAlias = "ColumnSchema"  # type: ignore


AdditionalMetric = namedtuple("AdditionalMetric", "metric column_names why_types")


class Resolver(ABC):
    """A resolver maps from a column name and a data type to trackers.

    Note that the key of the result dictionaries defines the namespaces of the metrics in the serialized form."""

    def __init__(self):
        self.additional_metrics: List[AdditionalMetric] = []

    def add_standard_metric(
        self,
        metric: StandardMetric,
        column_names: List[String] = [],
        why_types: List[Union[Fractional, Integral, String]] = [],
    ):
        if not column_names and not why_types:
            raise ValueError("Either column names or why types must not be empty.")
        if column_names and why_types:
            raise ValueError("column_names or why_types should be defined, not both.")
        if not isinstance(metric, StandardMetric):
            raise ValueError("Metric must be of StandardMetric type.")
        additional_metric = AdditionalMetric(metric=metric, column_names=column_names, why_types=why_types)
        self.additional_metrics.append(additional_metric)

    def resolve_additional_standard_metrics(
        self, result: Dict[str, Metric], name: str, why_type: DataType, column_schema: ColumnSchema
    ) -> Dict[str, Metric]:
        for additional_metric in self.additional_metrics:
            for column_name in additional_metric.column_names:
                if column_name == name and additional_metric.metric.name not in result:
                    result[additional_metric.metric.name] = additional_metric.metric.zero(column_schema.cfg)
            for target_why_type in additional_metric.why_types:
                if isinstance(target_why_type, type(why_type)) and additional_metric.metric.name not in result:
                    result[additional_metric.metric.name] = additional_metric.metric.zero(column_schema.cfg)
        return result

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

        if self.additional_metrics:
            result = self.resolve_additional_standard_metrics(
                result=result, name=name, why_type=why_type, column_schema=column_schema
            )

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

        if self.additional_metrics:
            result = self.resolve_additional_standard_metrics(
                result=result, name=name, why_type=why_type, column_schema=column_schema
            )

        return result


class HistogramCountingTrackingResolver(Resolver):
    """Resolver that only adds distribution tracker."""

    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        metrics: List[StandardMetric] = [StandardMetric.distribution]
        result: Dict[str, Metric] = {}
        for m in metrics:
            result[m.name] = m.zero(column_schema.cfg)

        if self.additional_metrics:
            result = self.resolve_additional_standard_metrics(
                result=result, name=name, why_type=why_type, column_schema=column_schema
            )

        return result
