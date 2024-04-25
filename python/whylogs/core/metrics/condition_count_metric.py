import logging
import re
from copy import copy
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Callable, Dict, List, Optional, Set, Union

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

logger = logging.getLogger(__name__)

# For backward compatability
Relation = Rel  # type: ignore


# relation() is annoying, use Predicate instead
def relation(op: Relation, value: Union[str, int, float]) -> Callable[[Any], bool]:  # type: ignore
    if op == Relation.match:  # type: ignore
        return lambda x: re.compile(value).match(x)  # type: ignore
    if op == Relation.fullmatch:  # type: ignore
        return lambda x: re.compile(value).fullmatch(x)  # type: ignore
    if op == Relation.equal:  # type: ignore
        return lambda x: x == value  # type: ignore
    if op == Relation.less:  # type: ignore
        return lambda x: x < value  # type: ignore
    if op == Relation.leq:  # type: ignore
        return lambda x: x <= value  # type: ignore
    if op == Relation.greater:  # type: ignore
        return lambda x: x > value  # type: ignore
    if op == Relation.geq:  # type: ignore
        return lambda x: x >= value  # type: ignore
    if op == Relation.neq:  # type: ignore
        return lambda x: x != value  # type: ignore
    raise ValueError("Unknown ConditionCountMetric predicate")


def and_relations(left: Callable[[Any], bool], right: Callable[[Any], bool]) -> Callable[[Any], bool]:
    return lambda x: left(x) and right(x)


def or_relations(left: Callable[[Any], bool], right: Callable[[Any], bool]) -> Callable[[Any], bool]:
    return lambda x: left(x) or right(x)


def not_relation(relation: Callable[[Any], bool]) -> Callable[[Any], bool]:
    return lambda x: not relation(x)


@dataclass(frozen=True)
class Condition:
    """
    Condition to be evaluated by the ConditionCountMetric.

    Parameters
    ----------
    relation: Callable[[Any], bool]
        The predicate to evaluate. The callable is passed a value from the column the
        ConditionCountMetric is attached to, and returns True if the value satisfies
        the condition.
    throw_on_failure: bool
        If throw_on_failure is true, whylogs will immediately raise a ValueError if
        data that does not satisfy the condition is logged.
    log_on_failure: bool
        If log_on_failure is true, whylogs will log a warning message if data that does not
        satisfy the conditon is logged.
    actions: List[Callable[[str, str, Any], None]]
        A list of callables that will be invoked if data that does not satisfy the conditon
        is logged. The arguments passed to the callable are the metric's name ("condition_count"),
        the name of the failed condition, and the value that caused the failure.
    """

    relation: Callable[[Any], bool]
    throw_on_failure: bool = False
    log_on_failure: bool = False
    actions: List[Callable[[str, str, Any], None]] = field(default_factory=list)


@dataclass(frozen=True)
class ConditionCountConfig(MetricConfig):
    conditions: Dict[str, Condition] = field(default_factory=dict)
    exclude_from_serialization: bool = False


@dataclass(frozen=True)
class ConditionCountMetric(Metric):
    """
    A whylogs metric that counts how many column entries satisfy a condition.

    Parameters
    ----------
    conditions: Dict[str, Condition]
        The conditions evaluated by the metric. The key is the condition name, and the
        Condition value specifies the Callable condition predicate to evaluate & count.

    Examples
    --------
    This example counts the occurrances of email addresses in the `some_text` column and
    credit card numbers in the `more_text` column.

    ```
    import pandas as pd
    import whylogs as why
    from whylogs.core.resolvers import STANDARD_RESOLVER
    from whylogs.core.specialized_resolvers import ConditionCountMetricSpec
    from whylogs.core.metrics.condition_count_metric import Condition
    from whylogs.core.relations import Predicate
    from whylogs.core.schema import DeclarativeSchema

    email_condition = {"contiansEmail": Condition(Predicate().fullmatch("[\\w.]+[\\._]?[a-z0-9]+[@]\\w+[.]\\w{2,3}"))}
    cc_condition = {"containsCreditCard": Condition(Predicate().matches(".*4[0-9]{12}(?:[0-9]{3})?"))}

    schema = DeclarativeSchema(STANDARD_RESOLVER)
    schema.add_resolver_spec(column_name="some_text", metrics=[ConditionCountMetricSpec(email_condition)])
    schema.add_resolver_spec(column_name="more_text", metrics=[ConditionCountMetricspec(cc_condition)])

    df = pd.DataFrame({"some_text": ["not an email", "bob@spam.com"], "more_text": ["frogs", "4000000000000"]})
    view = why.log(df).view()
    view.to_pandas()[['condition_count/containsEmail', 'condition_count/containsCreditCard', 'condition_count/total']]

    # results in

               condition_count/containsEmail   condition_count/containsCreditCard      condition_count/total
    column
    some_text                            1.0                                  NaN                          2
    more_text                            NaN                                  1.0                          2
    ```
    """

    conditions: Dict[str, Condition]
    total: IntegralComponent
    matches: Dict[str, IntegralComponent] = field(default_factory=dict)
    hide_from_serialization: bool = False

    @property
    def exclude_from_serialization(self) -> bool:
        return self.hide_from_serialization

    @property
    def namespace(self) -> str:
        return "condition_count"

    def __post_init__(self) -> None:
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

        return ConditionCountMetric(
            copy(self.conditions),
            IntegralComponent(total),
            matches,
            hide_from_serialization=self.hide_from_serialization or other.hide_from_serialization,
        )

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
        log_conditions: Set[str] = set()
        throw_conditions: Set[str] = set()
        for datum in list(chain.from_iterable(data.raw_iterator())):
            count += 1
            for cond_name, condition in self.conditions.items():
                try:
                    if condition.relation(datum):
                        self.matches[cond_name].set(self.matches[cond_name].value + 1)
                    else:
                        if condition.log_on_failure:
                            log_conditions.add(cond_name)
                        if condition.throw_on_failure:
                            throw_conditions.add(cond_name)
                        for action in condition.actions:
                            action(self.namespace, cond_name, datum)

                except Exception as e:  # noqa
                    logger.debug(e)

        self.total.set(self.total.value + count)
        if log_conditions:
            logger.warning(f"Conditions {', '.join(list(log_conditions))} failed")

        if throw_conditions:
            raise ValueError(f"Condition {', '.join(list(throw_conditions))} failed")

        return OperationResult.ok(count)

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "ConditionCountMetric":
        config = config or ConditionCountConfig()
        if not isinstance(config, ConditionCountConfig):
            raise ValueError("ConditionCountMetric.zero() requires ConditionCountConfig argument")

        metric = ConditionCountMetric(
            conditions=copy(config.conditions),
            total=IntegralComponent(0),
            hide_from_serialization=config.exclude_from_serialization,
        )
        return metric

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


# Register it so Multimetric and ProfileView can deserialize
register_metric(ConditionCountMetric)
