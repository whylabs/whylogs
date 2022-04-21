from abc import ABC, abstractmethod
from typing import Dict, List, TypeVar

from typing_extensions import TypeAlias

from whylogs.core.datatypes import DataType, Fractional, Integral, String
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.metrics import Metric

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
        metrics: List[StandardMetric] = [StandardMetric.cnt, StandardMetric.types]
        if isinstance(why_type, Integral):
            metrics.append(StandardMetric.dist)
            metrics.append(StandardMetric.ints)
            metrics.append(StandardMetric.card)
            metrics.append(StandardMetric.fi)
        elif isinstance(why_type, Fractional):
            metrics.append(StandardMetric.card)
            metrics.append(StandardMetric.dist)
        elif isinstance(why_type, String):
            metrics.append(StandardMetric.card)
            metrics.append(StandardMetric.fi)

        if column_schema.cfg.fi_disabled:
            metrics.remove(StandardMetric.fi)

        result: Dict[str, Metric] = {}
        for m in metrics:
            result[m.name] = m.zero(column_schema)
        return result
