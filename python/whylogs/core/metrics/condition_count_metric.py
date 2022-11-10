import logging
from copy import copy
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Dict, List, Optional, Set

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics.metric_components import IntegralComponent, MetricComponent
from whylogs.core.metrics.metrics import Metric, MetricConfig, OperationResult
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricMessage
from whylogs.core.relations import Predicate

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Condition:
    relation: Predicate
    throw_on_failure: bool = False
    log_on_failure: bool = False


@dataclass(frozen=True)
class ConditionCountConfig(MetricConfig):
    conditions: Dict[str, Condition] = field(default_factory=dict)


@dataclass(frozen=True)
class ConditionCountMetric(Metric):
    conditions: Dict[str, Condition]
    total: IntegralComponent
    matches: Dict[str, IntegralComponent] = field(default_factory=dict)

    @property
    def namespace(self) -> str:
        return "condition_count"

    def __post_init__(self) -> None:
        super(type(self), self).__post_init__()
        if "total" in self.conditions.keys():
            raise ValueError("Condition cannot be named 'total'")

        for cond_name in self.conditions.keys():
            if cond_name not in self.matches:
                self.matches[cond_name] = IntegralComponent(0)

    def merge(self, other: "ConditionCountMetric") -> "ConditionCountMetric":
        if set(self.matches.keys()) != set(other.matches.keys()):
            # log warning?
            matches = {cond_name: IntegralComponent(comp.value) for cond_name, comp in self.matches.items()}
            total = self.total.value
        else:
            matches = {
                cond_name: IntegralComponent(self.matches[cond_name].value + other.matches[cond_name].value)
                for cond_name in self.matches.keys()
            }
            total = self.total.value + other.total.value

        return ConditionCountMetric(copy(self.conditions), IntegralComponent(total), matches)

    def add_conditions(self, conditions: Dict[str, Condition]) -> None:
        if "total" in conditions.keys():
            raise ValueError("Condition cannot be named 'total'")
        for cond_name, cond in conditions.items():
            self.conditions[cond_name] = cond
            self.matches[cond_name] = IntegralComponent(0)

    def get_component_paths(self) -> List[str]:
        paths: List[str] = [
            "total",
        ] + list(self.conditions.keys())
        return paths

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        if data.len <= 0:
            return OperationResult.ok(0)

        count = 0
        failed_conditions: Set[str] = set()
        for datum in list(chain.from_iterable(data.raw_iterator())):
            count += 1
            for cond_name, condition in self.conditions.items():
                try:
                    if condition.relation(datum):
                        self.matches[cond_name].set(self.matches[cond_name].value + 1)
                    else:
                        failed_conditions.add(cond_name)

                except:  # noqa
                    failed_conditions.add(cond_name)

        self.total.set(self.total.value + count)
        if failed_conditions and condition.log_on_failure:
            logger.warning(f"Condition(s) {', '.join(failed_conditions)} failed")
        if failed_conditions and condition.throw_on_failure:
            raise ValueError(f"Condition(s) {', '.join(failed_conditions)} failed")
        return OperationResult.ok(count)

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "ConditionCountMetric":
        config = config or ConditionCountConfig()
        if not isinstance(config, ConditionCountConfig):
            raise ValueError("ConditionCountMetric.zero() requires ConditionCountConfig argument")

        return ConditionCountMetric(
            conditions=copy(config.conditions),
            total=IntegralComponent(0),
        )

    def to_protobuf(self) -> MetricMessage:
        msg = {"total": self.total.to_protobuf()}
        for cond_name in self.conditions.keys():
            msg[cond_name] = self.matches[cond_name].to_protobuf()

        return MetricMessage(metric_components=msg)

    def to_summary_dict(self, cfg: Optional[SummaryConfig] = None) -> Dict[str, Any]:
        summary = {"total": self.total.value}
        for cond_name in self.matches.keys():
            summary[cond_name] = self.matches[cond_name].value

        return summary

    @classmethod
    def from_protobuf(cls, msg: MetricMessage) -> "ConditionCountMetric":
        cond_names: Set[str] = set(msg.metric_components.keys())
        cond_names.remove("total")

        conditions = {cond_name: lambda x: False for cond_name in cond_names}
        total = MetricComponent.from_protobuf(msg.metric_components["total"])
        matches = {
            cond_name: MetricComponent.from_protobuf(msg.metric_components[cond_name]) for cond_name in cond_names
        }
        return ConditionCountMetric(
            conditions,
            total,
            matches,
        )
