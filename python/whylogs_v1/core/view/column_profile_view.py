import logging
from typing import Dict, Optional

from whylogs_v1.core.metrics import MergeableMetric, get_registry
from whylogs_v1.core.proto import ColumnMessage

logger = logging.getLogger(__name__)


class ColumnProfileView(object):
    _metrics: Dict[str, MergeableMetric]
    _registry = get_registry()

    def __init__(
        self,
        metrics: Dict[str, MergeableMetric],
        success_count: int = 0,
        failure_count: int = 0,
    ):
        self._metrics = metrics.copy()
        self._success_count = success_count
        self._failure_count = failure_count

    def merge(self, other: "ColumnProfileView") -> "ColumnProfileView":
        all_names = set(self._metrics.keys()).union(other._metrics.keys())
        merged_metrics: Dict[str, MergeableMetric] = {}
        for n in all_names:
            lhs = self._metrics.get(n)
            rhs = other._metrics.get(n)

            res = lhs
            if lhs is None:
                res = rhs
            elif rhs is not None:
                res = lhs + rhs
            assert res is not None
            merged_metrics[n] = res
        return ColumnProfileView(
            metrics=merged_metrics,
            success_count=self._success_count + other._success_count,
            failure_count=self._failure_count + other._failure_count,
        )

    def __add__(self, other: "ColumnProfileView") -> "ColumnProfileView":
        return self.merge(other)

    def get_metric(self, m_name: str) -> Optional[MergeableMetric]:
        return self._metrics.get(m_name)

    def serialize(self) -> ColumnMessage:
        res = {}
        for m_name, m in self._metrics.items():
            res[m_name] = m.serialize()
        return ColumnMessage(trackers=res)

    @classmethod
    def deserialize(cls, msg: ColumnMessage) -> "ColumnProfileView":
        metrics = {}
        for m_name, m in msg.trackers.items():
            mergeable_metric = cls._registry.get_mergeable_metric(m_name)
            metric = mergeable_metric.fromSerialized(m)
            metrics[m_name] = metric
        return ColumnProfileView(metrics=metrics)
