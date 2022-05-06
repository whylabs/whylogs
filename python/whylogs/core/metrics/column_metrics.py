from dataclasses import dataclass
from typing import Any, Dict

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics.metric_components import IntegralComponent
from whylogs.core.metrics.metrics import ColumnSchema, Metric, OperationResult
from whylogs.core.preprocessing import PreprocessedColumn


@dataclass(frozen=True)
class TypeCountersMetric(Metric):
    integral: IntegralComponent
    fractional: IntegralComponent
    boolean: IntegralComponent
    string: IntegralComponent
    object: IntegralComponent

    @property
    def namespace(self) -> str:
        return "types"

    def to_summary_dict(self, cfg: SummaryConfig) -> Dict[str, Any]:
        return {
            "integral": self.integral.value,
            "fractional": self.fractional.value,
            "boolean": self.boolean.value,
            "string": self.string.value,
            "object": self.object.value,
        }

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        if data.len <= 0:
            return OperationResult.ok(0)

        successes = 0

        integral_prev = self.integral.value
        integral = 0
        if data.numpy.ints is not None:
            integral += len(data.numpy.ints)
        elif data.list.ints is not None:
            integral += len(data.list.ints)
        successes += integral
        self.integral.set(integral + integral_prev)

        fractional = 0
        fractional_prev = self.fractional.value
        if data.numpy.floats is not None:
            fractional += len(data.numpy.floats)
        elif data.list.floats is not None:
            fractional += len(data.list.floats)
        successes += fractional
        self.fractional.set(fractional + fractional_prev)

        bool_count = self.boolean.value + data.bool_count
        successes += data.bool_count
        self.boolean.set(bool_count)

        string_count = 0
        string_count_prev = self.string.value
        if data.pandas.strings is not None:
            string_count += len(data.pandas.strings)
        elif data.list.strings is not None:
            string_count += len(data.list.strings)

        successes += string_count
        self.string.set(string_count + string_count_prev)

        objects = 0
        objects_prev = self.object.value
        if data.pandas.objs is not None:
            objects += len(data.pandas.objs)
        elif data.list.objs is not None:
            objects += len(data.list.objs)

        successes += objects
        self.object.set(objects + objects_prev)
        return OperationResult.ok(successes)

    @classmethod
    def zero(cls, schema: ColumnSchema) -> "TypeCountersMetric":
        return TypeCountersMetric(
            integral=IntegralComponent(0),
            fractional=IntegralComponent(0),
            boolean=IntegralComponent(0),
            string=IntegralComponent(0),
            object=IntegralComponent(0),
        )


@dataclass(frozen=True)
class ColumnCountsMetric(Metric):
    n: IntegralComponent
    null: IntegralComponent

    @property
    def namespace(self) -> str:
        return "counts"

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        n: int = self.n.value
        null: int = self.null.value
        if data.len <= 0:
            return OperationResult.ok(0)
        n += data.len
        self.n.set(n)

        null += data.null_count
        self.null.set(null)
        return OperationResult.ok(data.len)

    def to_summary_dict(self, cfg: SummaryConfig) -> Dict[str, Any]:
        return {"n": self.n.value, "null": self.null.value}

    @classmethod
    def zero(cls, schema: ColumnSchema) -> "ColumnCountsMetric":
        return ColumnCountsMetric(n=IntegralComponent(0), null=IntegralComponent(0))
