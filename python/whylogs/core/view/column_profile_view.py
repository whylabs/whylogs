import logging
from typing import Any, Dict, Optional

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.metrics import Metric
from whylogs.core.proto import ColumnMessage, MetricComponentMessage, MetricMessage

logger = logging.getLogger(__name__)


class ColumnProfileView(object):
    _metrics: Dict[str, Metric]

    def __init__(
        self,
        metrics: Dict[str, Metric],
        success_count: int = 0,
        failure_count: int = 0,
    ):
        self._metrics = metrics.copy()
        self._success_count = success_count
        self._failure_count = failure_count

    def merge(self, other: "ColumnProfileView") -> "ColumnProfileView":
        all_names = set(self._metrics.keys()).union(other._metrics.keys())
        merged_metrics: Dict[str, Metric] = {}
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

    def get_metric(self, m_name: str) -> Optional[Metric]:
        return self._metrics.get(m_name)

    def serialize(self) -> ColumnMessage:
        res: Dict[str, MetricComponentMessage] = {}
        for m_name, m in self._metrics.items():
            res.update(m.serialize().metric_components)
        return ColumnMessage(metric_components=res)

    def to_summary_dict(
        self, *, column_metric: Optional[str] = None, cfg: Optional[SummaryConfig] = None
    ) -> Dict[str, Any]:
        if cfg is None:
            cfg = SummaryConfig()

        res = {}
        for metric_name, metric in self._metrics.items():
            if column_metric is not None and metric_name != column_metric:
                continue
            try:
                m_sum = metric.to_summary_dict(cfg=cfg)
                for k, v in m_sum.items():
                    res[f"{metric_name}/{k}"] = v
            except NotImplementedError:
                logger.warning(f"No summary implemented for {metric_name}")
                pass

        if column_metric is not None and len(res) == 0:
            raise ValueError(f"No metric available for requested column metric: {column_metric}")

        return res

    @classmethod
    def deserialize(cls, msg: ColumnMessage) -> "ColumnProfileView":
        result_metrics: Dict[str, Metric] = {}
        metric_messages: Dict[str, Dict[str, MetricComponentMessage]] = {}
        for full_path, c_msg in msg.metric_components.items():
            metric_name = full_path.split("/")[0]
            metric_components = metric_messages.get(metric_name)

            if metric_components is None:
                metric_components = {}
            metric_messages[metric_name] = metric_components

            c_key = full_path[len(metric_name) + 1 :]
            metric_components[c_key] = c_msg

        for m_name, metric_components in metric_messages.items():
            m_enum = StandardMetric.__members__.get(m_name)
            if m_enum is None:
                raise ValueError(f"Unsupported metric: {m_name}")

            m_msg = MetricMessage(metric_components=metric_components)
            result_metrics[m_name] = m_enum.value.deserialize(m_msg)
        return ColumnProfileView(metrics=result_metrics)
