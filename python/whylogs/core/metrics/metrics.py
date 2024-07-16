import dataclasses
import math
import statistics
import sys
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar, Union

import whylogs_sketching as ds  # type: ignore
from google.protobuf.struct_pb2 import Struct

import whylogs.core.configs as conf
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
from whylogs.core.stubs import pd as pd

T = TypeVar("T")
M = TypeVar("M", bound=MetricComponent)
NUM = TypeVar("NUM", float, int)

METRIC = TypeVar("METRIC", bound="Metric")


@dataclass(frozen=True)
class MetricConfig:
    hll_lg_k: int = field(default_factory=lambda: conf.hll_lg_k)
    kll_k: int = field(default_factory=lambda: conf.kll_k)
    fi_lg_max_k: int = field(default_factory=lambda: conf.fi_lg_max_k)
    fi_disabled: bool = field(default_factory=lambda: conf.fi_disabled)
    track_unicode_ranges: bool = field(default_factory=lambda: conf.track_unicode_ranges)
    large_kll_k: bool = field(default_factory=lambda: conf.large_kll_k)
    kll_k_large: int = field(default_factory=lambda: conf.kll_k_large)
    unicode_ranges: Dict[str, Tuple[int, int]] = field(default_factory=lambda: dict(conf.unicode_ranges))
    lower_case: bool = field(default_factory=lambda: conf.lower_case)
    normalize: bool = field(default_factory=lambda: conf.normalize)
    max_frequent_item_size: int = field(default_factory=lambda: conf.max_frequent_item_size)
    identity_column: Optional[str] = field(default_factory=lambda: conf.identity_column)


_METRIC_DESERIALIZER_REGISTRY: Dict[str, Type[METRIC]] = {}  # type: ignore


def custom_metric(metric: Type[METRIC]) -> Type[METRIC]:  # type: ignore
    global _METRIC_DESERIALIZER_REGISTRY
    _METRIC_DESERIALIZER_REGISTRY[metric.get_namespace()] = metric
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
    @property
    def exclude_from_serialization(self) -> bool:
        return False

    @classmethod
    # TODO: deprecate config argument
    def get_namespace(cls, config: Optional[MetricConfig] = None) -> str:
        return cls.zero().namespace

    @property
    @abstractmethod
    def namespace(self) -> str:
        raise NotImplementedError

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
    def to_summary_dict(self, cfg: Optional[conf.SummaryConfig] = None) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def zero(cls: Type[METRIC], config: Optional[MetricConfig] = None) -> METRIC:
        pass

    @classmethod
    def from_protobuf(cls: Type[METRIC], msg: MetricMessage) -> METRIC:
        if not dataclasses.is_dataclass(cls):
            raise ValueError(f"Metric class: {cls} is not a dataclass")

        components: Dict[str, MetricComponent] = {}
        for k, m in msg.metric_components.items():
            components[k] = MetricComponent.from_protobuf(m)

        return cls(**components)


def register_metric(metrics: Union[Metric, Type[METRIC], List[Metric], List[Type[METRIC]]]) -> None:
    global _METRIC_DESERIALIZER_REGISTRY
    if not isinstance(metrics, list):
        metrics = [metrics]  # type: ignore

    for metric in metrics:  # type: ignore
        _METRIC_DESERIALIZER_REGISTRY[metric.get_namespace()] = metric  # type: ignore


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
        if data.numpy.ints is not None and len(data.numpy.ints) > 0:
            max_ = max([max_, data.numpy.ints.max()])
            min_ = min([min_, data.numpy.ints.min()])
            successes += len(data.numpy.ints)

        if data.list.ints is not None and len(data.list.ints) > 0:
            l_max = max(data.list.ints)
            l_min = min(data.list.ints)
            max_ = max([max_, l_max])
            min_ = min([min_, l_min])
            successes += len(data.list.ints)

        self.max.set(max_)
        self.min.set(min_)
        return OperationResult.ok(successes)

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "IntsMetric":
        return IntsMetric(max=MaxIntegralComponent(-sys.maxsize), min=MinIntegralComponent(sys.maxsize))

    def to_summary_dict(self, cfg: Optional[conf.SummaryConfig] = None) -> Dict[str, Union[int, float, str, None]]:
        return {"max": self.maximum, "min": self.minimum}

    @property
    def maximum(self) -> float:
        return self.max.value

    @property
    def minimum(self) -> float:
        return self.min.value


register_metric(IntsMetric)


@dataclass(frozen=True)
class DistributionMetric(Metric):
    kll: KllComponent
    mean: FractionalComponent
    m2: FractionalComponent

    @property
    def namespace(self) -> str:
        return "distribution"

    def to_summary_dict(self, cfg: Optional[conf.SummaryConfig] = None) -> Dict[str, Union[int, float, str, None]]:
        if self.n == 0:
            quantiles = [None, None, None, None, None, None, None, None, None]
        else:
            quantiles = self.kll.value.get_quantiles([0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
        return {
            "mean": self.avg,
            "stddev": self.stddev,
            "n": self.kll.value.get_n(),
            "max": self.kll.value.get_max_value(),
            "min": self.kll.value.get_min_value(),
            "q_01": quantiles[0],
            "q_05": quantiles[1],
            "q_10": quantiles[2],
            "q_25": quantiles[3],
            "median": quantiles[4],
            "q_75": quantiles[5],
            "q_90": quantiles[6],
            "q_95": quantiles[7],
            "q_99": quantiles[8],
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
                    self.kll.value.update(array=arr)
                    n_b = len(arr)
                    if n_b > 1:
                        n_b = len(arr)
                        mean_b = arr.mean()
                        m2_b = arr.var(ddof=1) * (n_b - 1)

                        second = VarianceM2Result(n=n_b, mean=mean_b, m2=m2_b)
                        first = parallel_variance_m2(first=first, second=second)
                    elif n_b == 1:
                        # fall back to https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
                        # #Weighted_incremental_algorithm
                        if isinstance(arr, pd.Series):
                            first = welford_online_variance_m2(existing=first, new_value=arr.iloc[0])
                        else:
                            first = welford_online_variance_m2(existing=first, new_value=arr[0])

        for lst in [view.list.ints, view.list.floats]:
            if lst is not None and len(lst) > 0:
                self.kll.value.update_list(num_items=lst)
                n_b = len(lst)
                if n_b > 1:
                    mean_b = statistics.mean(lst)
                    m2_b = statistics.variance(lst) * (n_b - 1)
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

        delta = other.avg - self.avg
        new_n = a_n + b_n
        if a_n != 0 or b_n != 0:
            m2 = self.m2.value + other.m2.value + delta**2 * a_n * b_n / new_n

            mean = (a_n / new_n) * (self.avg) + (b_n / new_n) * (other.avg)
        else:
            m2 = 0
            mean = 0

        # merge the sketch
        kll = self.kll + other.kll

        return DistributionMetric(kll=kll, mean=FractionalComponent(mean), m2=FractionalComponent(m2))

    @property
    def n(self) -> float:
        return self.kll.value.get_n()

    @property
    def variance(self) -> float:
        """Returns the sample variance of the stream.

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

    def get_quantile(self, quantile: float) -> Optional[float]:
        result = None
        if quantile < 0 or quantile > 1:
            raise ValueError(f"quantile={quantile} is not supported, please specify a value between 0 and 1 inclusive.")
        if self.n > 0:
            result = self.kll.value.get_quantiles([quantile])[0]
        return result

    @property
    def median(self) -> Optional[float]:
        return self.get_quantile(0.5)

    @property
    def q_01(self) -> Optional[float]:
        return self.get_quantile(0.01)

    @property
    def q_05(self) -> Optional[float]:
        return self.get_quantile(0.05)

    @property
    def q_10(self) -> Optional[float]:
        return self.get_quantile(0.1)

    @property
    def q_25(self) -> Optional[float]:
        return self.get_quantile(0.25)

    @property
    def q_75(self) -> Optional[float]:
        return self.get_quantile(0.75)

    @property
    def q_90(self) -> Optional[float]:
        return self.get_quantile(0.9)

    @property
    def q_95(self) -> Optional[float]:
        return self.get_quantile(0.95)

    @property
    def q_99(self) -> Optional[float]:
        return self.get_quantile(0.99)

    @property
    def max(self) -> float:
        if self.kll.value.is_empty():
            return -sys.float_info.max
        return self.kll.value.get_max_value()

    @property
    def min(self) -> float:
        if self.kll.value.is_empty():
            return sys.float_info.max
        return self.kll.value.get_min_value()

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "DistributionMetric":
        config = config or MetricConfig()
        configured_kll_k = config.kll_k_large if config.large_kll_k else config.kll_k
        sk = ds.kll_doubles_sketch(k=configured_kll_k)
        return DistributionMetric(
            kll=KllComponent(sk),
            mean=FractionalComponent(0.0),
            m2=FractionalComponent(0.0),
        )


register_metric(DistributionMetric)


@dataclass(frozen=True)
class FrequentItem:
    value: str
    est: int
    upper: int
    lower: int


@dataclass(frozen=True)
class FrequentItemsMetric(Metric):
    frequent_strings: FrequentStringsComponent
    max_frequent_item_size: int = 128

    @property
    def namespace(self) -> str:
        return "frequent_items"

    def columnar_update(self, view: PreprocessedColumn) -> OperationResult:
        successes = 0
        for arr in [view.numpy.floats, view.numpy.ints]:
            if arr is not None:
                self.frequent_strings.value.update_np(arr)
                successes += len(arr)
        # TODO: Include strings in above when we update whylogs-sketching fi.update_np to take strings
        if view.numpy.strings is not None:
            self.frequent_strings.value.update_str_list(view.numpy.strings.tolist())
            successes += len(view.numpy.strings)
        if view.pandas.strings is not None:
            strings = [s[0 : self.max_frequent_item_size] for s in view.pandas.strings.to_list()]
            self.frequent_strings.value.update_str_list(strings)
            successes += len(view.pandas.strings)

        if view.list.ints is not None:
            self.frequent_strings.value.update_int_list(view.list.ints)
            successes += len(view.list.ints)

        if view.list.floats is not None:
            self.frequent_strings.value.update_double_list(view.list.floats)
            successes += len(view.list.floats)

        if view.list.strings is not None:
            strings = [s[0 : self.max_frequent_item_size] for s in view.list.strings]
            self.frequent_strings.value.update_str_list(strings)
            successes += len(view.list.strings)

        failures = 0
        if view.list.objs is not None:
            successes += len(view.list.objs)
        if view.pandas.objs is not None:
            successes += len(view.pandas.objs)

        return OperationResult(successes=successes, failures=failures)

    def to_summary_dict(self, cfg: Optional[conf.SummaryConfig] = None) -> Dict[str, Any]:
        cfg = cfg or conf.SummaryConfig()
        all_freq_items = self.frequent_strings.value.get_frequent_items(
            cfg.frequent_items_error_type.to_datasketches_type()
        )
        if cfg.frequent_items_limit > 0:
            limited_freq_items = all_freq_items[: cfg.frequent_items_limit]
        else:
            limited_freq_items = all_freq_items
        items = [FrequentItem(value=x[0], est=x[1], lower=x[2], upper=x[3]) for x in limited_freq_items]
        return {"frequent_strings": items}

    @property
    def strings(self) -> List[FrequentItem]:
        if self.frequent_strings.value.is_empty():
            return []
        summary = self.to_summary_dict()
        return summary["frequent_strings"]

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "FrequentItemsMetric":
        config = config or MetricConfig()
        sk = ds.frequent_strings_sketch(lg_max_k=config.fi_lg_max_k)
        return FrequentItemsMetric(
            frequent_strings=FrequentStringsComponent(sk), max_frequent_item_size=config.max_frequent_item_size
        )


register_metric(FrequentItemsMetric)


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
            if view.numpy.strings is not None:
                self.hll.value.update_str_list(view.numpy.strings.tolist())
                successes += len(view.numpy.strings)

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

        # special handling for bool counts
        if view.bool_count > 0:
            if view.bool_count_where_true > 0 and view.bool_count > view.bool_count_where_true:
                self.hll.value.update_int_list([1, 0])
            elif view.bool_count_where_true > 0:
                self.hll.value.update(1)
            else:
                self.hll.value.update(0)
            successes += view.bool_count

        failures = 0
        if view.list.objs:
            failures = len(view.list.objs)
        return OperationResult(successes=successes, failures=failures)

    def to_summary_dict(self, cfg: Optional[conf.SummaryConfig] = None) -> Dict[str, Any]:
        cfg = cfg or conf.SummaryConfig()
        return {
            "est": self.hll.value.get_estimate(),
            f"upper_{cfg.hll_stddev}": self.hll.value.get_upper_bound(cfg.hll_stddev),
            f"lower_{cfg.hll_stddev}": self.hll.value.get_lower_bound(cfg.hll_stddev),
        }

    @property
    def estimate(self) -> Optional[float]:
        result = None
        if not self.hll.value.is_empty():
            result = self.hll.value.get_estimate()

        return result

    def get_upper_bound(self, number_of_standard_deviations: int) -> Optional[float]:
        result = None
        if not self.hll.value.is_empty():
            result = self.hll.value.get_upper_bound(number_of_standard_deviations)

        return result

    def get_lower_bound(self, number_of_standard_deviations: int) -> Optional[float]:
        result = None
        if not self.hll.value.is_empty():
            result = self.hll.value.get_lower_bound(number_of_standard_deviations)

        return result

    @property
    def upper_1(self) -> Optional[float]:
        return self.get_upper_bound(1)

    @property
    def lower_1(self) -> Optional[float]:
        return self.get_lower_bound(1)

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "CardinalityMetric":
        config = config or MetricConfig()
        sk = ds.hll_sketch(config.hll_lg_k)
        return CardinalityMetric(hll=HllComponent(sk))


register_metric(CardinalityMetric)


def _drop_private_fields(data: List[Tuple[str, Any]]) -> Dict[str, Any]:
    return {k: v for k, v in data if not k.startswith("_")}


_STRUCT_NAME = "dataclass_param"


class CustomMetricBase(Metric, ABC):
    """
    You can use this as a base class for custom metrics that don't use
    the supplied or custom MetricComponents. Subclasses must be decorated with
    @dataclass. All fields not prefixed with an underscore will be included
    in the summary and will be [de]serialized. Such subclasses will need to
    implement the namespace, merge, and zero methods. They should be registered
    by calling register_metric()
    """

    def get_component_paths(self) -> List[str]:
        return [_STRUCT_NAME]  # Assumes everything to be serde'd will be in the Struct

    def to_summary_dict(self, cfg: Optional[conf.SummaryConfig] = None) -> Dict[str, Any]:
        return asdict(self, dict_factory=_drop_private_fields)

    def to_protobuf(self) -> MetricMessage:
        mcm = MetricComponentMessage(dataclass_param=Struct())
        mcm.dataclass_param.update(asdict(self, dict_factory=_drop_private_fields))
        return MetricMessage(metric_components={_STRUCT_NAME: mcm})

    @classmethod
    def from_protobuf(cls: Type[METRIC], msg: MetricMessage) -> METRIC:
        my_struct = msg.metric_components[_STRUCT_NAME].dataclass_param
        return cls(**my_struct)


@dataclass
class CardinalityThresholds:
    few: int = 50
    proportionately_few: float = 0.01
