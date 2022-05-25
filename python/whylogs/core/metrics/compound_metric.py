from abc import ABC
from typing import Any, Dict, List, Type, TypeVar

from whylogs.core.configs import SummaryConfig
from whylogs.core.errors import DeserializationError, UnsupportedError
from whylogs.core.metrics import Metric
from whylogs.core.metrics.metrics import _METRIC_DESERIALIZER_REGISTRY, OperationResult
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricComponentMessage, MetricMessage

COMPOUND_METRIC = TypeVar("COMPOUND_METRIC", bound="CompoundMetric")


class CompoundMetric(Metric, ABC):
    """
    CompoundMetric serves as a base class for custom metrics that consist
    of one or more metrics. It is handy when you need to do some
    processing of the logged values and track serveral metrics on
    the results. The sub-metrics must either be a StandardMetric, or tagged
    as a @custom_metric or @dataclass. Note that CompoundMetric is neither, so it
    cannot be nested.

    Typically you will need to override namespace(); columnar_update(), calling
    it on the submetrics as needed; and the zero() method to return an
    appropriate "empty" instance of your metric. You will need to override from_protobuf()
    and merge() if your subclass __init__() method takes arguments different than
    CompoundMetrtic's. You can use the submetrics_from_protbuf() and merge_submetrics()
    helper methods to implement them. The CompoundMetric class will handle the rest of
    the Metric interface. Don't use / or : in the subclass' namespace.

    See UnicodeRangeMetric for an example.
    """

    submetrics: Dict[str, Metric]

    def __init__(self, submetrics: Dict[str, Metric]):
        """
        Parameters
        ----------
        submetrics : Dict[str, Metric]
            The collection of metrics that comprise the CompoundMetric.
            The key servers as the name of the sub-metric. E.g., the
            metric summary entries will have keys of the form
               "<namespace>/<submetric name>/<component name>"
            Submetric names should only contain alphanumeric characters,
            hyphens, and underscores.
        """

        if ":" in self.namespace or "/" in self.namespace:
            raise ValueError(f"Invalid namespace {self.namespace}")
        for sub_name, submetric in submetrics.items():
            if ":" in sub_name or "/" in sub_name:
                raise ValueError(f"Invalid submetric name {sub_name}")

        self.submetrics = submetrics.copy()

    def merge_submetrics(self: COMPOUND_METRIC, other: COMPOUND_METRIC) -> Dict[str, Metric]:
        if self.namespace != other.namespace:
            raise ValueError(f"Attempt to merge CompoundMetrics {self.namespace} and {other.namespace}")
        if self.submetrics.keys() != other.submetrics.keys():
            raise ValueError("Attempt to merge incompatible CompoundMetrics")
        return {name: (self.submetrics[name] + other.submetrics[name]) for name in self.submetrics.keys()}

    def merge(self: COMPOUND_METRIC, other: COMPOUND_METRIC) -> COMPOUND_METRIC:
        return self.__class__(self.merge_submetrics(other))

    def to_protobuf(self) -> MetricMessage:
        msg = {}
        for sub_name, submetric in self.submetrics.items():
            sub_msg = submetric.to_protobuf()
            for comp_name, comp_msg in sub_msg.metric_components.items():
                msg[sub_name + ":" + submetric.namespace + "/" + comp_name] = comp_msg
        return MetricMessage(metric_components=msg)

    def get_component_paths(self) -> List[str]:
        res = []
        for sub_name, submetric in self.submetrics.items():
            for comp_name in submetric.get_component_paths():
                res.append(sub_name + ":" + submetric.namespace + "/" + comp_name)
        return res

    def to_summary_dict(self, cfg: SummaryConfig) -> Dict[str, Any]:
        summary = {}
        for sub_name, submetric in self.submetrics.items():
            sub_summary = submetric.to_summary_dict(cfg)
            for comp_name, comp_msg in sub_summary.items():
                summary["/".join([self.namespace, sub_name, comp_name])] = comp_msg
        return summary

    def columnar_update(self, view: PreprocessedColumn) -> OperationResult:
        result = OperationResult()
        for submetric in self.submetrics.values():
            result = result + submetric.columnar_update(view)
        return result

    @classmethod
    def submetrics_from_protobuf(cls: Type[COMPOUND_METRIC], msg: MetricMessage) -> Dict[str, Metric]:
        submetrics: Dict[str, Metric] = {}
        submetric_msgs: Dict[str, Dict[str, MetricComponentMessage]] = {}
        for key, comp_msg in msg.metric_components.items():
            submetric_name, comp_name = key.split("/")
            if submetric_msgs.get(submetric_name) is None:
                submetric_msgs[submetric_name] = {}
            submetric_msgs[submetric_name][comp_name] = comp_msg

        for m_name_and_type, metric_components in submetric_msgs.items():
            m_name, m_type = m_name_and_type.split(":")
            if m_type in _METRIC_DESERIALIZER_REGISTRY:
                metric_class = _METRIC_DESERIALIZER_REGISTRY[m_type]
            else:
                raise UnsupportedError(f"Unsupported metric: {m_type}")

            m_msg = MetricMessage(metric_components=metric_components)
            try:
                submetrics[m_name] = metric_class.from_protobuf(m_msg)
            except:  # noqa
                raise DeserializationError(f"Failed to deserialize metric: {m_name}")
        return submetrics

    @classmethod
    def from_protobuf(cls: Type[COMPOUND_METRIC], msg: MetricMessage) -> COMPOUND_METRIC:
        return cls(cls.submetrics_from_protobuf(msg))
