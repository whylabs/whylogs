from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics.metric_components import IntegralComponent
from whylogs.core.metrics.metrics import (
    Metric,
    MetricConfig,
    OperationResult,
    register_metric,
)
from whylogs.core.preprocessing import PreprocessedColumn


@dataclass(frozen=True)
class TypeCountersMetric(Metric):
    integral: IntegralComponent
    fractional: IntegralComponent
    boolean: IntegralComponent
    string: IntegralComponent
    object: IntegralComponent
    tensor: IntegralComponent = field(default_factory=lambda: IntegralComponent(0))

    @property
    def namespace(self) -> str:
        return "types"

    def to_summary_dict(self, cfg: Optional[SummaryConfig] = None) -> Dict[str, Any]:
        return {
            "integral": self.integral.value,
            "fractional": self.fractional.value,
            "boolean": self.boolean.value,
            "string": self.string.value,
            "object": self.object.value,
            "tensor": self.tensor.value,
        }

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        if data.len <= 0:
            return OperationResult.ok(0)

        successes = 0

        integral_prev = self.integral.value
        integral = 0
        if data.numpy.ints is not None:
            integral += len(data.numpy.ints)
        if data.list.ints is not None:
            integral += len(data.list.ints)
        successes += integral
        self.integral.set(integral + integral_prev)

        fractional = 0
        fractional_prev = self.fractional.value
        if data.numpy.floats is not None:
            fractional += len(data.numpy.floats)
        if data.list.floats is not None:
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
        if data.list.strings is not None:
            string_count += len(data.list.strings)
        if data.numpy.strings is not None:
            string_count += len(data.numpy.strings)
        successes += string_count
        self.string.set(string_count + string_count_prev)

        tensor_count = 0
        tensor_count_prev = self.tensor.value
        if data.pandas.tensors is not None:
            tensor_count += len(data.pandas.tensors)
        if data.list.tensors is not None:
            tensor_count += len(data.list.tensors)

        successes += tensor_count
        self.tensor.set(tensor_count + tensor_count_prev)

        objects = 0
        objects_prev = self.object.value
        if data.pandas.objs is not None:
            objects += len(data.pandas.objs)
        if data.list.objs is not None:
            objects += len(data.list.objs)

        successes += objects
        self.object.set(objects + objects_prev)
        return OperationResult.ok(successes)

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "TypeCountersMetric":
        return TypeCountersMetric(
            integral=IntegralComponent(0),
            fractional=IntegralComponent(0),
            boolean=IntegralComponent(0),
            string=IntegralComponent(0),
            object=IntegralComponent(0),
            tensor=IntegralComponent(0),
        )


@dataclass(frozen=True)
class ColumnCountsMetric(Metric):
    n: IntegralComponent
    null: IntegralComponent
    nan: IntegralComponent = field(default_factory=lambda: IntegralComponent(0))
    inf: IntegralComponent = field(default_factory=lambda: IntegralComponent(0))

    @property
    def namespace(self) -> str:
        return "counts"

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        n: int = self.n.value
        null: int = self.null.value
        nan: int = self.nan.value
        inf: int = self.inf.value

        if data.len <= 0:
            return OperationResult.ok(0)
        n += data.len
        self.n.set(n)

        null += data.null_count
        self.null.set(null)

        nan += data.nan_count
        self.nan.set(nan)

        inf += data.inf_count
        self.inf.set(inf)

        return OperationResult.ok(data.len)

    def to_summary_dict(self, cfg: Optional[SummaryConfig] = None) -> Dict[str, Any]:
        return {"n": self.n.value, "null": self.null.value, "nan": self.nan.value, "inf": self.inf.value}

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "ColumnCountsMetric":
        return ColumnCountsMetric(
            n=IntegralComponent(0), null=IntegralComponent(0), nan=IntegralComponent(0), inf=IntegralComponent(0)
        )


register_metric([TypeCountersMetric, ColumnCountsMetric])
