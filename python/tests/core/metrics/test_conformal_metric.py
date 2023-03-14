from typing import Optional

import numpy as np
import pandas as pd

import whylogs as why
from whylogs.core.metrics import IntsMetric, MetricConfig
from whylogs.core.metrics.conformal_metric import ConformalMetric
from whylogs.core.metrics.metric_components import (
    MaxIntegralComponent,
    MinIntegralComponent,
)
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.resolvers import MetricSpec, ResolverSpec
from whylogs.core.schema import DeclarativeSchema
from whylogs.core.view.dataset_profile_view import DatasetProfileView


def test_conformal_metric_deserialization() -> None:
    ints = IntsMetric.zero(MetricConfig())
    data = list(range(0, 100))
    arr = np.array(data)
    col = PreprocessedColumn.apply(arr)
    ints.columnar_update(col)
    msg = ints.to_protobuf()

    chameleon = ConformalMetric.deserialize(ints.namespace, msg)
    assert chameleon.namespace == ints.namespace
    ints_sum = ints.to_summary_dict()
    chameleon_sum = chameleon.to_summary_dict()
    assert ints_sum.keys() == chameleon_sum.keys()
    for k, v in chameleon_sum.items():
        assert ints_sum[k] == v


def test_conformal_metric_merge() -> None:
    ints1 = IntsMetric.zero(MetricConfig())
    ints2 = IntsMetric.zero(MetricConfig())
    col1 = PreprocessedColumn.apply(np.array(list(range(0, 100))))
    col2 = PreprocessedColumn.apply(np.array(list(range(100, 200))))
    ints1.columnar_update(col1)
    ints2.columnar_update(col2)
    merged_ints = ints1.merge(ints2)

    msg = ints1.to_protobuf()
    chameleon1 = ConformalMetric.deserialize(ints1.namespace, msg)
    merged_chameleon = chameleon1.merge(ints2)
    assert merged_ints.to_summary_dict() == merged_chameleon.to_summary_dict()

    msg = ints2.to_protobuf()
    chameleon2 = ConformalMetric.deserialize(ints2.namespace, msg)
    merged_chameleon = ints1.merge(chameleon2)
    assert merged_ints.to_summary_dict() == merged_chameleon.to_summary_dict()

    merged_chameleon = chameleon1.merge(chameleon2)
    assert merged_ints.to_summary_dict() == merged_chameleon.to_summary_dict()


class UnknownMetric(IntsMetric):
    """
    The UnknownMetric is not registered, so it must be deserialized
    as a ConformalMetric.
    """

    @property
    def namespace(self) -> str:
        return "unknown"

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "UnknownMetric":
        return cls(MaxIntegralComponent(0), MinIntegralComponent(0))


def test_unknown_metric_deserialization() -> None:
    assert UnknownMetric.get_namespace() == "unknown"
    assert UnknownMetric.zero().namespace == "unknown"

    schema = DeclarativeSchema([ResolverSpec(column_name="col1", metrics=[MetricSpec(UnknownMetric)])])
    data = list(range(0, 100))
    df = pd.DataFrame(data={"col1": data})
    view = why.log(pandas=df, schema=schema).view()
    serialized = view.serialize()
    rehydrated = DatasetProfileView.deserialize(serialized)
    column = rehydrated.get_column("col1")
    metric = column.get_metric("unknown")
    assert isinstance(metric, ConformalMetric)
    assert metric.max.value == max(data)
    assert metric.min.value == min(data)
