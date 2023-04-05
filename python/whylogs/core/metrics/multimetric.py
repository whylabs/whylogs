import logging
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any, Dict, List, Optional, Type, TypeVar

from whylogs.core.configs import SummaryConfig
from whylogs.core.datatypes import DataType
from whylogs.core.errors import DeserializationError, UnsupportedError
from whylogs.core.metrics import Metric
from whylogs.core.metrics.metric_components import MetricComponent
from whylogs.core.metrics.metrics import _METRIC_DESERIALIZER_REGISTRY, OperationResult
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricComponentMessage, MetricMessage

logger = logging.getLogger(__name__)


class SubmetricSchema(ABC):
    @abstractmethod
    def resolve(self, name: str, why_type: DataType, fi_disabled: bool = False) -> Dict[str, Metric]:
        raise NotImplementedError


MULTI_METRIC = TypeVar("MULTI_METRIC", bound="MultiMetric")


def _do_submetric_merge(lhs: Dict[str, Metric], rhs: Dict[str, Metric]) -> Dict[str, Metric]:
    namespaces = set(lhs.keys())
    namespaces.update(rhs.keys())
    result: Dict[str, Metric] = {}
    for namespace in namespaces:
        if namespace in lhs and namespace in rhs:
            result[namespace] = lhs[namespace] + rhs[namespace]
        elif namespace in lhs:
            result[namespace] = deepcopy(lhs[namespace])
        else:
            result[namespace] = deepcopy(rhs[namespace])

    return result


class MultiMetric(Metric, ABC):
    """
    MultiMetric serves as a base class for custom metrics that consist
    of one or more metrics. It is handy when you need to do some
    processing of the logged values and track several metrics on
    the results. The sub-metrics must either be a StandardMetric, or tagged
    as a @custom_metric or registered via register_metric(). Note that
    MultiMetric is neither, so it cannot be nested.

    Typically you will need to override namespace(); columnar_update(), calling
    it on the submetrics as needed; and the zero() method to return an
    appropriate "empty" instance of your metric. You will need to override from_protobuf()
    and merge() if your subclass __init__() method takes arguments different than
    MultiMetrtic's. You can use the submetrics_from_protbuf() and merge_submetrics()
    helper methods to implement them. The MultiMetric class will handle the rest of
    the Metric interface. Don't use / or : in the subclass' namespace.

    See UnicodeRangeMetric for an example.
    """

    submetrics: Dict[str, Dict[str, Metric]]

    def __init__(self, submetrics: Dict[str, Dict[str, Metric]]):
        """
                Parameters
                ----------
                submetrics : Dict[str, Dict[str, Metric]]
                    The collection of metrics that comprise the MultiMetric.

        TODO: type this

                       "<submetric name>/<component name>"
                    Submetric names should only contain alphanumeric characters,
                    hyphens, and underscores.
        """

        if ":" in self.namespace or "/" in self.namespace:
            raise ValueError(f"Invalid namespace {self.namespace}")
        for sub_name in submetrics.keys():
            if ":" in sub_name or "/" in sub_name:
                raise ValueError(f"Invalid submetric name {sub_name}")

        self.submetrics = deepcopy(submetrics)

    def merge_submetrics(self: MULTI_METRIC, other: MULTI_METRIC) -> Dict[str, Dict[str, Metric]]:
        if self.namespace != other.namespace:
            raise ValueError(f"Attempt to merge MultiMetrics {self.namespace} and {other.namespace}")

        submetric_names = set(self.submetrics.keys())
        submetric_names.update(other.submetrics.keys())
        submetrics: Dict[str, Dict[str, Metric]] = dict()
        for submetric_name in submetric_names:
            if submetric_name in self.submetrics and submetric_name in other.submetrics:
                submetrics[submetric_name] = _do_submetric_merge(
                    self.submetrics[submetric_name], other.submetrics[submetric_name]
                )
            elif submetric_name in self.submetrics:
                submetrics[submetric_name] = deepcopy(self.submetrics[submetric_name])
            else:
                submetrics[submetric_name] = deepcopy(other.submetrics[submetric_name])

        return submetrics

    def merge(self: MULTI_METRIC, other: MULTI_METRIC) -> MULTI_METRIC:
        return self.__class__(self.merge_submetrics(other))

    def to_protobuf(self) -> MetricMessage:
        msg = {}
        for sub_name, metrics in self.submetrics.items():
            for namespace, metric in metrics.items():
                sub_msg = metric.to_protobuf()
                for comp_name, comp_msg in sub_msg.metric_components.items():
                    msg[f"{sub_name}:{namespace}/{comp_name}"] = comp_msg

        return MetricMessage(metric_components=msg)

    def get_component_paths(self) -> List[str]:
        res = []
        for sub_name, metrics in self.submetrics.items():
            for namespace, metric in metrics.items():
                for comp_name in metric.get_component_paths():
                    res.append(f"{sub_name}:{namespace}/{comp_name}")

        for k, v in self.__dict__.items():  # Grab any components that aren't in submetrics
            if not isinstance(v, MetricComponent):
                continue
            res.append(k)

        return res

    def to_summary_dict(self, cfg: Optional[SummaryConfig] = None) -> Dict[str, Any]:
        cfg = cfg or SummaryConfig()
        summary = {}
        for sub_name, metrics in self.submetrics.items():
            for namespace, metric in metrics.items():
                sub_summary = metric.to_summary_dict(cfg)
                for comp_name, comp_msg in sub_summary.items():
                    summary[f"{sub_name}:{namespace}/{comp_name}"] = comp_msg

        return summary

    def columnar_update(self, view: PreprocessedColumn) -> OperationResult:
        result = OperationResult()
        for sub_name, metrics in self.submetrics.items():
            for metric in metrics.values():
                result = result + metric.columnar_update(view)

        return result

    @classmethod
    def submetrics_from_protobuf(cls: Type[MULTI_METRIC], msg: MetricMessage) -> Dict[str, Dict[str, Metric]]:
        submetrics: Dict[str, Dict[str, Metric]] = {}
        submetric_msgs: Dict[str, Dict[str, Dict[str, MetricComponentMessage]]] = {}
        for key, comp_msg in msg.metric_components.items():
            sub_name_and_type, comp_name = key.split("/")
            sub_name, namespace = sub_name_and_type.split(":")
            if submetric_msgs.get(sub_name) is None:
                submetric_msgs[sub_name] = {}
            if submetric_msgs[sub_name].get(namespace) is None:
                submetric_msgs[sub_name][namespace] = {}
            submetric_msgs[sub_name][namespace][comp_name] = comp_msg

        for sub_name, metric_msgs in submetric_msgs.items():
            for namespace, metric_components in metric_msgs.items():
                if namespace in _METRIC_DESERIALIZER_REGISTRY:
                    metric_class = _METRIC_DESERIALIZER_REGISTRY[namespace]
                else:
                    raise UnsupportedError(f"Unsupported metric: {namespace}")

                metric_msg = MetricMessage(metric_components=metric_components)
                try:
                    if submetrics.get(sub_name) is None:
                        submetrics[sub_name] = {}
                    submetrics[sub_name][namespace] = metric_class.from_protobuf(metric_msg)
                except Exception as e:  # noqa
                    logger.exception(e)
                    raise DeserializationError(f"Failed to deserialize metric: {sub_name}:{namespace}")

        return submetrics

    @classmethod
    def from_protobuf(cls: Type[MULTI_METRIC], msg: MetricMessage) -> MULTI_METRIC:
        return cls(cls.submetrics_from_protobuf(msg))
