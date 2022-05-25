import dataclasses
import math
import statistics
import sys
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Tuple, Type, TypeVar, Union

import whylogs_sketching as ds  # type: ignore
from google.protobuf.struct_pb2 import Struct
from typing_extensions import TypeAlias

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics.maths import (
    VarianceM2Result,
    parallel_variance_m2,
    welford_online_variance_m2,
)
from whylogs.core.metrics.metric_components import (
    FractionalComponent,
    FrequentStringsComponent,
    HllComponent,
    KllComponent,
    MaxIntegralComponent,
    MetricComponent,
    MinIntegralComponent,
)
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricComponentMessage, MetricMessage

T = TypeVar("T")
M = TypeVar("M", bound=MetricComponent)
NUM = TypeVar("NUM", float, int)

METRIC = TypeVar("METRIC", bound="Metric")
ColumnSchema: TypeAlias = "ColumnSchema"  # type: ignore


_METRIC_DESERIALIZER_REGISTRY: Dict[str, Type[METRIC]] = {}  # type: ignore


def custom_metric(metric: Type[METRIC], schema=None) -> Type[METRIC]:  # type: ignore
    global _METRIC_DESERIALIZER_REGISTRY
    _METRIC_DESERIALIZER_REGISTRY[metric.get_namespace(schema)] = metric
    return metric


@dataclass(frozen=True)
class OperationResult:
    failures: int = 0
    successes: int = 0

    @classmethod
    def ok(cls, cnt: int = 1) -> "OperationResult":
        return OperationResult(successes=cnt, failures=0)

    @classmethod
    def failed(cls, cnt: int = 1) -> "OperationResult":
        return OperationResult(successes=0, failures=cnt)

    def __add__(self, other: "OperationResult") -> "OperationResult":
        return OperationResult(
            successes=self.successes + other.successes,
            failures=self.failures + other.failures,
        )


class Metric(ABC):
    @classmethod
    def get_namespace(cls, schema: ColumnSchema) -> str:
        return cls.zero(schema).namespace

    @property
    @abstractmethod
    def namespace(self) -> str:
        raise NotImplementedError

    def __post_init__(self):
        global _METRIC_DESERIALIZER_REGISTRY
        _METRIC_DESERIALIZER_REGISTRY[self.namespace] = self.__class__

    def __add__(self: METRIC, other: METRIC) -> METRIC:
        return self.merge(other)

    def merge(self: METRIC, other: METRIC) -> METRIC:
        res: Dict[str, MetricComponent] = {}
        for k, v in self.__dict__.items():
            if isinstance(v, MetricComponent):
                res[k] = v + other.__dict__[k]

        return self.__class__(**res)

    def to_protobuf(self) -> MetricMessage:
        if not dataclasses.is_dataclass(self):
            raise ValueError("Metric object is not a dataclass")

        res = {}
        for k, v in self.__dict__.items():
            if not isinstance(v, MetricComponent):
                continue
            res[k] = v.to_protobuf()
        return MetricMessage(metric_components=res)

    def get_component_paths(self) -> List[str]:
        res = []
        for k, v in self.__dict__.items():
            if not isinstance(v, MetricComponent):
                continue
            res.append(k)
        return res

    @abstractmethod
    def to_summary_dict(self, cfg: SummaryConfig) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def zero(cls: Type[METRIC], schema: ColumnSchema) -> METRIC:
        pass

    @classmethod
    def from_protobuf(cls: Type[METRIC], msg: MetricMessage) -> METRIC:
        if not dataclasses.is_dataclass(cls):
            raise ValueError(f"Metric class: {cls} is not a dataclass")

        components: Dict[str, MetricComponent] = {}
        for k, m in msg.metric_components.items():
            components[k] = MetricComponent.from_protobuf(m)

        return cls(**components)


@dataclass(frozen=True)
class IntsMetric(Metric):
    max: MaxIntegralComponent
    min: MinIntegralComponent

    @property
    def namespace(self) -> str:
        return "ints"

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        if data.len <= 0:
            return OperationResult.ok(0)

        successes = 0

        max_ = self.max.value
        min_ = self.min.value
        if data.numpy.ints is not None:
            max_ = max([max_, data.numpy.ints.max()])
            min_ = min([min_, data.numpy.ints.min()])
            successes += len(data.numpy.ints)

        if data.list.ints is not None:
            l_max = max(data.list.ints)
            l_min = min(data.list.ints)
            max_ = max([max_, l_max])
            min_ = min([min_, l_min])
            successes += len(data.list.ints)

        self.max.set(max_)
        self.min.set(min_)
        return OperationResult.ok(successes)

    @classmethod
    def zero(cls, schema: ColumnSchema) -> "IntsMetric":
        return IntsMetric(max=MaxIntegralComponent(-sys.maxsize), min=MinIntegralComponent(sys.maxsize))

    def to_summary_dict(self, cfg: SummaryConfig) -> Dict[str, Union[int, float, str, None]]:
        return {"max": self.max.value, "min": self.min.value}


@dataclass(frozen=True)
class DistributionMetric(Metric):
    kll: KllComponent
    mean: FractionalComponent
    m2: FractionalComponent

    @property
    def namespace(self) -> str:
        return "distribution"

    def to_summary_dict(self, cfg: SummaryConfig) -> Dict[str, Union[int, float, str, None]]:
        if self.n == 0:
            quantiles = [None, None, None, None, None]
        else:
            quantiles = self.kll.value.get_quantiles([0.1, 0.25, 0.5, 0.75, 0.9])
        return {
            "mean": self.mean.value,
            "stddev": self.stddev,
            "n": self.kll.value.get_n(),
            "max": self.kll.value.get_max_value(),
            "min": self.kll.value.get_min_value(),
            "q_10": quantiles[0],
            "q_25": quantiles[1],
            "median": quantiles[2],
            "q_75": quantiles[3],
            "q_90": quantiles[4],
        }

    def columnar_update(self, view: PreprocessedColumn) -> OperationResult:
        """
        Update the operation

        Algorithm: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm

        Args:
            view: the preprocessed column

        Returns:
            how many successful operations we had
        """
        successes = 0

        first = VarianceM2Result(n=self.kll.value.get_n(), mean=self.mean.value, m2=self.m2.value)

        if view.numpy.len > 0:
            for arr in [view.numpy.floats, view.numpy.ints]:
                if arr is not None:
                    self.kll.value.update(arr)
                    n_b = len(arr)
                    if n_b > 1:
                        n_b = len(arr)
                        mean_b = arr.mean()
                        m2_b = arr.var() * (n_b - 1)

                        second = VarianceM2Result(n=n_b, mean=mean_b, m2=m2_b)
                        first = parallel_variance_m2(first=first, second=second)
                    else:
                        # fall back to https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
                        # #Weighted_incremental_algorithm
                        first = welford_online_variance_m2(existing=first, new_value=first[0])

        for lst in [view.list.ints, view.list.floats]:
            if lst is not None and len(lst) > 0:
                self.kll.value.update_list(lst)
                n_b = len(lst)
                if n_b > 1:
                    mean_b = statistics.mean(lst)
                    m2_b = statistics.pvariance(lst) * (n_b - 1)
                    second = VarianceM2Result(n=n_b, mean=mean_b, m2=m2_b)

                    first = parallel_variance_m2(first=first, second=second)
                else:
                    first = welford_online_variance_m2(existing=first, new_value=lst[0])

        self.mean.set(first.mean)
        self.m2.set(first.m2)

        return OperationResult.ok(successes)

    def merge(self, other: "DistributionMetric") -> "DistributionMetric":
        # calculate mean and m2
        a_n = self.kll.value.get_n()
        b_n = other.kll.value.get_n()

        delta = other.mean.value - self.mean.value
        new_n = a_n + b_n
        m2 = self.m2.value + other.m2.value + delta**2 * a_n * b_n / new_n

        mean = (a_n / new_n) * (self.mean.value) + (b_n / new_n) * (other.mean.value)

        # merge the sketch
        kll = self.kll + other.kll

        return DistributionMetric(kll=kll, mean=FractionalComponent(mean), m2=FractionalComponent(m2))

    @property
    def n(self) -> float:
        return self.kll.value.get_n()

    @property
    def variance(self) -> float:
        """Returns the population variance of the stream.

        https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
        """
        if self.n <= 1:
            return float(0)
        return self.m2.value / (self.kll.value.get_n() - 1)

    @property
    def stddev(self) -> float:
        if self.n <= 1:
            return float(0)
        return math.sqrt(self.variance)

    @property
    def avg(self) -> float:
        return self.mean.value

    @property
    def max(self) -> float:
        return self.kll.value.get_max_value()

    @property
    def min(self) -> float:
        return self.kll.value.get_min_value()

    @classmethod
    def zero(cls, schema: ColumnSchema) -> "DistributionMetric":
        sk = ds.kll_doubles_sketch(k=schema.cfg.kll_k)
        return DistributionMetric(
            kll=KllComponent(sk),
            mean=FractionalComponent(0.0),
            m2=FractionalComponent(0.0),
        )


@dataclass(frozen=True)
class FrequentItem:
    value: str
    est: int
    upper: int
    lower: int


@dataclass(frozen=True)
class FrequentItemsMetric(Metric):
    frequent_strings: FrequentStringsComponent

    @property
    def namespace(self) -> str:
        return "frequent_items"

    def columnar_update(self, view: PreprocessedColumn) -> OperationResult:
        successes = 0
        for arr in [view.numpy.floats, view.numpy.ints]:
            if arr is not None:
                self.frequent_strings.value.update_np(arr)
                successes += len(arr)
        if view.pandas.strings is not None:
            self.frequent_strings.value.update_str_list(view.pandas.strings.to_list())
            successes += len(view.pandas.strings)

        if view.list.ints is not None:
            self.frequent_strings.value.update_int_list(view.list.ints)
            successes += len(view.list.ints)

        if view.list.floats is not None:
            self.frequent_strings.value.update_double_list(view.list.floats)
            successes += len(view.list.floats)

        if view.list.strings is not None:
            self.frequent_strings.value.update_str_list(view.list.strings)
            successes += len(view.list.strings)

        failures = 0
        if view.list.objs is not None:
            successes += len(view.list.objs)
        if view.pandas.objs is not None:
            successes += len(view.pandas.objs)

        return OperationResult(successes=successes, failures=failures)

    def to_summary_dict(self, cfg: SummaryConfig) -> Dict[str, Any]:
        all_freq_items = self.frequent_strings.value.get_frequent_items(
            cfg.frequent_items_error_type.to_datasketches_type()
        )
        if cfg.frequent_items_limit > 0:
            limited_freq_items = all_freq_items[: cfg.frequent_items_limit]
        else:
            limited_freq_items = all_freq_items
        items = [FrequentItem(value=x[0], est=x[1], upper=x[2], lower=x[3]) for x in limited_freq_items]
        return {"frequent_strings": items}

    @classmethod
    def zero(cls, schema: ColumnSchema) -> "FrequentItemsMetric":
        sk = ds.frequent_strings_sketch(lg_max_k=schema.cfg.fi_lg_max_k)
        return FrequentItemsMetric(frequent_strings=FrequentStringsComponent(sk))


@dataclass(frozen=True)
class CardinalityMetric(Metric):
    hll: HllComponent

    @property
    def namespace(self) -> str:
        return "cardinality"

    def columnar_update(self, view: PreprocessedColumn) -> OperationResult:
        successes = 0
        if view.numpy.len > 0:
            if view.numpy.ints is not None:
                self.hll.value.update_np(view.numpy.ints)
                successes += len(view.numpy.ints)
            if view.numpy.floats is not None:
                self.hll.value.update_np(view.numpy.floats)
                successes += len(view.numpy.floats)
        if view.pandas.strings is not None:
            self.hll.value.update_str_list(view.pandas.strings.to_list())
            successes += len(view.pandas.strings)

        # update everything in the remaining lists
        if view.list.ints:
            self.hll.value.update_int_list(view.list.ints)
            successes += len(view.list.ints)
        if view.list.floats:
            self.hll.value.update_double_list(view.list.floats)
            successes += len(view.list.floats)
        if view.list.strings:
            self.hll.value.update_str_list(view.list.strings)
            successes += len(view.list.strings)

        failures = 0
        if view.list.objs:
            failures = len(view.list.objs)
        return OperationResult(successes=successes, failures=failures)

    def to_summary_dict(self, cfg: SummaryConfig) -> Dict[str, Any]:
        return {
            "est": self.hll.value.get_estimate(),
            f"upper_{cfg.hll_stddev}": self.hll.value.get_upper_bound(cfg.hll_stddev),
            f"lower_{cfg.hll_stddev}": self.hll.value.get_lower_bound(cfg.hll_stddev),
        }

    @classmethod
    def zero(cls, schema: ColumnSchema) -> "CardinalityMetric":
        sk = ds.hll_sketch(schema.cfg.hll_lg_k)
        return CardinalityMetric(hll=HllComponent(sk))


def _drop_private_fields(data: List[Tuple[str, Any]]) -> Dict[str, Any]:
    return {k: v for k, v in data if not k.startswith("_")}


_STRUCT_NAME = "dataclass_param"


class CustomMetricBase(Metric, ABC):
    """
    You can use this as a base class for custom metrics that don't use
    the supplied or custom MetricComponents. Subclasses must be decorated with
    @dataclass. All fields not prefixed with an underscore will be included
    in the summary and will be [de]serialized. Such subclasses will need to
    implement the namespace, merge, and zero methods.
    """

    def get_component_paths(self) -> List[str]:
        return [_STRUCT_NAME]  # Assumes everything to be serde'd will be in the Struct

    def to_summary_dict(self, cfg: SummaryConfig) -> Dict[str, Any]:
        return asdict(self, dict_factory=_drop_private_fields)

    def to_protobuf(self) -> MetricMessage:
        mcm = MetricComponentMessage(dataclass_param=Struct())
        mcm.dataclass_param.update(asdict(self, dict_factory=_drop_private_fields))
        return MetricMessage(metric_components={_STRUCT_NAME: mcm})

    @classmethod
    def from_protobuf(cls: Type[METRIC], msg: MetricMessage) -> METRIC:
        my_struct = msg.metric_components[_STRUCT_NAME].dataclass_param
        return cls(**my_struct)
