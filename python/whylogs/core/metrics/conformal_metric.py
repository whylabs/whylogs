from dataclasses import dataclass
from typing import Any, Dict, Optional

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics.metric_components import MetricComponent
from whylogs.core.metrics.metrics import Metric, MetricConfig, OperationResult
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricMessage


@dataclass(frozen=True)
class ConformalMetric(Metric):
    _namespace: str

    @classmethod
    def get_namespace(cls, config: Optional[MetricConfig] = None) -> str:
        raise NotImplementedError

    @property
    def namespace(self) -> str:
        return self._namespace

    # NOTE: This only works for metrics for which merge is strictly component-wise merging.
    def merge(self, other: Metric) -> "ConformalMetric":
        if other.namespace != self._namespace:
            raise ValueError(f"Attempted to merge {self._namespace} with incompatible {other.namespace} metric")

        res = ConformalMetric(self._namespace)
        components = set(self.__dict__.keys())
        components.update(set(other.__dict__.keys()))
        print(type(components))
        for component in components:
            if component not in other.__dict__:
                res.__dict__[component] = self.__dict__[component]
            elif component not in self.__dict__ and isinstance(other.__dict__[component], MetricComponent):
                res.__dict__[component] = other.__dict__[component]
            elif isinstance(other.__dict__[component], MetricComponent):
                res.__dict__[component] = self.__dict__[component] + other.__dict__[component]

        return res

    # TODO: move summarization to MetricComponent
    def to_summary_dict(self, cfg: Optional[SummaryConfig] = None) -> Dict[str, Any]:
        summary: Dict[str, Any] = {}
        for k, v in self.__dict__.items():
            if not isinstance(v, MetricComponent):
                continue
            summary[k] = v.value

        return summary

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        raise NotImplementedError

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "ConformalMetric":
        raise NotImplementedError

    @classmethod
    def from_protobuf(cls, msg: MetricMessage) -> "ConformalMetric":
        raise NotImplementedError

    @classmethod
    def deserialize(cls, namespace: str, msg: MetricMessage) -> "ConformalMetric":
        chameleon = cls(namespace)
        for k, m in msg.metric_components.items():
            chameleon.__dict__[k] = MetricComponent.from_protobuf(m)

        return chameleon
