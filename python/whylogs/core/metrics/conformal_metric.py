from dataclasses import dataclass
from typing import Any, Dict, Optional

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics.metric_components import IntegralComponent, MetricComponent
from whylogs.core.metrics.metrics import (
    Metric,
    MetricConfig,
    OperationResult,
    register_metric,
)
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricMessage
from whylogs.core.relations import Relation as Rel


@dataclass(frozen=True)
class ConformalMetric(Metric):
    _namespace: str

    @classmethod
    def get_namespace(cls, config: Optional[MetricConfig] = None) -> str:
        raise NotImplmentedError

    @property
    def namespace(self) -> str:
        return self._namespace

    def merge(self, other: "ConformalMetric") -> "ConformalMetric":
        if other.namespace != self._namespace:
            raise ValueError(f"Attempted to merge {self._namespace} with incompatible {other.namespace} metric")

        # TODO: handle symmetric set difference
        res = ConformalMetric(self._namespace)
        for k, v in self.__dict__.items():
            if isinstance(v, MetricComponent):
                res.__dict__[k] = v + other.__dict__[k]

        return res

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
        chameleon = ConformalMetric(namespace)
        for k, m in msg.metric_components.items():
            chameleon.__dict__[k] = MetricComponent.from_protobuf(m)

        return chameleon
