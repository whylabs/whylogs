from typing import Any, Dict, Type, TypeVar

from whylogs.core.configs import SummaryConfig
from whylogs.core.errors import UnsupportedError
from whylogs.core.metrics import Metric, StandardMetric
from whylogs.core.metrics.metrics import OperationResult
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricComponentMessage, MetricMessage

COMPOUND_METRIC = TypeVar("COMPOUND_METRIC", bound="CompoundMetric")


class CompoundMetric(Metric):
    """
    CompoundMetric serves as a base class for custom metrics that consist
    of one or more StandardMetrics. It is handy when you need to do some
    processing of the logged values and track serveral StandardMetrics on
    the results.

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

            Note that the sub-metrics must be one of the elements of
            the StandardMetric enumeration. CompoundMetric is not one
            of the elements, so it cannot contain nested CompoundMetrics.
        """
        if ":" in self.namespace or "/" in self.namespace:
            raise ValueError(f"Invalid namespace {self.namespace}")
        for sub_name, submetric in submetrics.items():
            if ":" in sub_name or "/" in sub_name:
                raise ValueError(f"Invalid submetric name {sub_name}")
            if StandardMetric.__members__.get(submetric.namespace) is None:
                raise ValueError(f"Submetric {sub_name} is not a StandardMetric")

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
                msg["/".join([self.namespace, sub_name + ":" + submetric.namespace, comp_name])] = comp_msg
        return MetricMessage(metric_components=msg)

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
        namespace: str
        for key, comp_msg in msg.metric_components.items():
            namespace, submetric_name, comp_name = key.split("/")
            if submetric_msgs.get(submetric_name) is None:
                submetric_msgs[submetric_name] = {}
            submetric_msgs[submetric_name][comp_name] = comp_msg

        for m_name_and_type, metric_components in submetric_msgs.items():
            m_name, m_type = m_name_and_type.split(":")
            m_enum = StandardMetric.__members__.get(m_type)
            if m_enum is None:
                raise UnsupportedError(f"Unsupported metric: {m_name}:{m_type}")
            m_msg = MetricMessage(metric_components=metric_components)
            submetrics[m_name] = m_enum.value.from_protobuf(m_msg)

        return submetrics

    @classmethod
    def from_protobuf(cls: Type[COMPOUND_METRIC], msg: MetricMessage) -> COMPOUND_METRIC:
        return cls(cls.submetrics_from_protobuf(msg))
