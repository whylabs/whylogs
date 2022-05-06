import datetime
from typing import Dict

import whylogs_sketching as ds  # type: ignore

from whylogs.core import ColumnProfileView, DatasetProfileView
from whylogs.core.errors import DeserializationError
from whylogs.core.metrics import (
    ColumnCountsMetric,
    DistributionMetric,
    FrequentItemsMetric,
    IntsMetric,
    StandardMetric,
    TypeCountersMetric,
)
from whylogs.core.metrics.metric_components import (
    FractionalComponent,
    FrequentStringsComponent,
    IntegralComponent,
    KllComponent,
    MaxIntegralComponent,
    MinIntegralComponent,
)
from whylogs.core.proto.v0 import ColumnMessageV0, DatasetProfileMessageV0, InferredType
from whylogs.core.utils import read_delimited_protobuf


def read_v0_to_view(path: str) -> DatasetProfileView:
    with open(path, "r+b") as f:
        v0_msg = read_delimited_protobuf(f, DatasetProfileMessageV0)
        if v0_msg is None:
            raise DeserializationError("Unexpected empty message")
        return v0_to_v1_view(v0_msg)


def v0_to_v1_view(msg: DatasetProfileMessageV0) -> DatasetProfileView:
    columns: Dict[str, ColumnProfileView] = {}

    dataset_timestamp = datetime.datetime.fromtimestamp(msg.properties.data_timestamp / 1000.0)
    creation_timestamp = datetime.datetime.fromtimestamp(msg.properties.session_timestamp / 1000.0)

    for col_name, col_msg in msg.columns.items():
        dist_metric = _extract_dist_metric(col_msg)
        fs = FrequentStringsComponent(ds.frequent_strings_sketch.deserialize(col_msg.frequent_items.sketch))
        fi_metric = FrequentItemsMetric(frequent_strings=fs)
        count_metrics = _extract_col_counts(col_msg)
        type_counters_metric = _extract_type_counts_metric(col_msg)
        int_metric = _extract_ints_metric(col_msg)

        columns[col_name] = ColumnProfileView(
            metrics={
                StandardMetric.distribution.name: dist_metric,
                StandardMetric.frequent_items.name: fi_metric,
                StandardMetric.counts.name: count_metrics,
                StandardMetric.types.name: type_counters_metric,
                StandardMetric.cardinality.name: type_counters_metric,
                StandardMetric.ints.name: int_metric,
            }
        )

    return DatasetProfileView(
        columns=columns, dataset_timestamp=dataset_timestamp, creation_timestamp=creation_timestamp
    )


def _extract_ints_metric(msg: ColumnMessageV0) -> IntsMetric:
    int_max = msg.numbers.longs.max
    int_min = msg.numbers.longs.min
    return IntsMetric(max=MaxIntegralComponent(int_max), min=MinIntegralComponent(int_min))


def _extract_type_counts_metric(msg: ColumnMessageV0) -> TypeCountersMetric:
    int_count = msg.schema.typeCounts.get(InferredType.INTEGRAL)
    bool_count = msg.schema.typeCounts.get(InferredType.BOOLEAN)
    frac_count = msg.schema.typeCounts.get(InferredType.FRACTIONAL)
    string_count = msg.schema.typeCounts.get(InferredType.STRING)
    obj_count = msg.schema.typeCounts.get(InferredType.UNKNOWN)
    return TypeCountersMetric(
        integral=IntegralComponent(int_count or 0),
        fractional=IntegralComponent(frac_count or 0),
        boolean=IntegralComponent(bool_count or 0),
        string=IntegralComponent(string_count or 0),
        object=IntegralComponent(obj_count or 0),
    )


def _extract_col_counts(msg: ColumnMessageV0) -> ColumnCountsMetric:
    count_n = msg.counters.count
    count_null = msg.counters.null_count
    return ColumnCountsMetric(n=IntegralComponent(count_n or 0), null=IntegralComponent(count_null.value or 0))


def _extract_dist_metric(msg: ColumnMessageV0) -> DistributionMetric:
    floats_sk = ds.kll_floats_sketch.deserialize(msg.numbers.histogram)
    doubles_sk: ds.kll_doubles_sketch = ds.kll_floats_sketch.float_to_doubles(floats_sk)
    dist_mean = msg.numbers.variance.mean
    dist_m2 = msg.numbers.variance.sum
    return DistributionMetric(
        kll=KllComponent(doubles_sk),
        mean=FractionalComponent(dist_mean),
        m2=FractionalComponent(dist_m2),
    )
