import logging
from typing import Any, Dict, List, Optional, TypeVar

from whylogs.core.configs import SummaryConfig
from whylogs.core.errors import DeserializationError, UnsupportedError
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.metrics import _METRIC_DESERIALIZER_REGISTRY, Metric
from whylogs.core.proto import ColumnMessage, MetricComponentMessage, MetricMessage

logger = logging.getLogger(__name__)

METRIC = TypeVar("METRIC", bound=Metric)


class ColumnProfileView(object):
    def __init__(
        self,
        metrics: Dict[str, METRIC],
        success_count: int = 0,
        failure_count: int = 0,
    ):
        self._metrics: Dict[str, METRIC] = metrics.copy()
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

    def __getstate__(self) -> bytes:
        return self.serialize()

    def __setstate__(self, serialized_profile: bytes):
        profile = ColumnProfileView.deserialize(serialized_profile)
        self.__dict__.update(profile.__dict__)

    def serialize(self) -> bytes:
        return self.to_protobuf().SerializeToString()

    @classmethod
    def deserialize(cls, serialized_profile: bytes) -> "ColumnProfileView":
        column_message = ColumnMessage.FromString(serialized_profile)
        return ColumnProfileView.from_protobuf(column_message)

    def get_metric(self, m_name: str) -> Optional[METRIC]:
        return self._metrics.get(m_name)

    def to_protobuf(self) -> ColumnMessage:
        res: Dict[str, MetricComponentMessage] = {}
        for m_name, m in self._metrics.items():
            for mc_name, mc in m.to_protobuf().metric_components.items():
                res[f"{m.namespace}/{mc_name}"] = mc
        return ColumnMessage(metric_components=res)

    def get_metric_component_paths(self) -> List[str]:
        res: List[str] = []
        for m_name, m in self._metrics.items():
            for mc_name in m.get_component_paths():
                res.append(f"{m.namespace}/{mc_name}")
        return res

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

        if column_metric is not None and len(res) == 0:
            raise UnsupportedError(f"No metric available for requested column metric: {column_metric}")

        return res

    @classmethod
    def zero(cls, msg: ColumnMessage) -> "ColumnProfileView":
        return ColumnProfileView(metrics={})

    @classmethod
    def from_protobuf(cls, msg: ColumnMessage) -> "ColumnProfileView":
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
                if m_name in _METRIC_DESERIALIZER_REGISTRY:
                    metric_class = _METRIC_DESERIALIZER_REGISTRY[m_name]
                else:
                    raise UnsupportedError(f"Unsupported metric: {m_name}")
            else:
                metric_class = m_enum.value

            m_msg = MetricMessage(metric_components=metric_components)
            try:
                result_metrics[m_name] = metric_class.from_protobuf(m_msg)
            except:  # noqa
                raise DeserializationError(f"Failed to deserialize metric: {m_name}")
        return ColumnProfileView(metrics=result_metrics)

    @classmethod
    def from_bytes(cls, data: bytes) -> "ColumnProfileView":
        msg = ColumnMessage()
        msg.ParseFromString(data)
        return ColumnProfileView.from_protobuf(msg)
