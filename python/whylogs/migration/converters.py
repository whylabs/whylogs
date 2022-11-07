import datetime
from logging import getLogger
from typing import Dict, List, Optional, Tuple

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
from whylogs.core.metrics.metrics import CardinalityMetric
from whylogs.core.proto import SegmentTag
from whylogs.core.proto.v0 import (
    ColumnMessageV0,
    CountersV0,
    DatasetProfileMessageV0,
    DatasetPropertiesV0,
    FrequentItemsSketchMessageV0,
    HllSketchMessageV0,
    InferredType,
    NumbersMessageV0,
    SchemaMessageV0,
    VarianceMessage,
)
from whylogs.core.segment import Segment
from whylogs.core.segmentation_partition import SegmentationPartition
from whylogs.core.utils import read_delimited_protobuf
from whylogs.core.utils.timestamp_calculations import to_utc_milliseconds

_DEFAULT_V0_LG_MAX_K = 12
_DEFAULT_V0_KLL_K = 128
_DEFAULT_V0_FI_LG_K = 10

EMPTY_FI_SKETCH: bytes = ds.frequent_strings_sketch(_DEFAULT_V0_FI_LG_K).serialize()
EMPTY_HLL: bytes = ds.hll_sketch(_DEFAULT_V0_LG_MAX_K).serialize_compact()
EMPTY_KLL: bytes = ds.kll_doubles_sketch(k=_DEFAULT_V0_KLL_K).serialize()
_empty_theta_union = ds.theta_union()
_empty_theta_union.update(ds.update_theta_sketch())
EMPTY_THETA: bytes = _empty_theta_union.get_result().serialize()

logger = getLogger(__name__)

PARTITION_ID = "segp_id"
PARTITION_NAME = "segp_name"
PARTITION_HAS_FILTER = "segp_filter"
SEGMENT_ON_COLUMN = "segp_col"
SEGMENT_ON_COLUMNS = "segp_cols"
_TAG_PREFIX = "whylogs.tag."


def _generate_segment_tags_metadata(
    segment: Segment, partition: SegmentationPartition
) -> Tuple[Dict[str, str], List[SegmentTag], Dict[str, str]]:
    segment_metadata: Optional[Dict[str, str]] = None
    segment_tags: Optional[List[SegmentTag]] = None
    if partition.mapper and partition.mapper.col_names:
        segment_metadata = {}
        segment_metadata[PARTITION_ID] = partition.id
        segment_metadata[PARTITION_NAME] = partition.name
        if partition.filter:
            segment_metadata[PARTITION_HAS_FILTER] = partition.filter_id

        segment_tags = []
        col_names = partition.mapper.col_names

        for index, column_name in enumerate(col_names):
            segment_tags.append(SegmentTag(key=_TAG_PREFIX + column_name, value=segment.key[index]))
    else:
        raise NotImplementedError(
            "serialization of custom v1 segmented profiles not yet supported! please use columnar segmentation. "
            f"Found partition: {partition} and segments: {segment}"
        )

    segment_message_tags: Dict[str, str] = {}
    for segment_tag in segment_tags:
        segment_message_tags[segment_tag.key] = segment_tag.value

    if partition.mapper.map:
        raise NotImplementedError(
            "custom mapping function segment serialization not yet supported, please use column based segmentation: "
            f"for partition: {partition} and segments: {segment}"
        )

    if partition.mapper.col_names:
        segment_columns = partition.mapper.col_names
        if len(segment_columns) == 1:
            segment_metadata[SEGMENT_ON_COLUMN] = segment_columns[0]
        elif len(segment_columns) > 1:
            segment_metadata[SEGMENT_ON_COLUMNS] = "".join(["(" + str(col) + ")" for col in segment_columns])

    return segment_message_tags, segment_tags, segment_metadata


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

    if msg.properties.tags:
        logger.info(
            f"Found tags in v0 message, ignoring while converting to v1 DatasetProfileView: {msg.properties.tags}"
        )

    return DatasetProfileView(
        columns=columns, dataset_timestamp=dataset_timestamp, creation_timestamp=creation_timestamp
    )


def _extract_ints_metric(msg: ColumnMessageV0) -> IntsMetric:
    int_max = msg.numbers.longs.max
    int_min = msg.numbers.longs.min
    return IntsMetric(max=MaxIntegralComponent(int_max), min=MinIntegralComponent(int_min))


def _extract_numbers_message_v0(col_prof: ColumnProfileView) -> NumbersMessageV0:
    distribution_metric: DistributionMetric = col_prof.get_metric(DistributionMetric.get_namespace())
    variance_message = VarianceMessage()
    msg_v0 = NumbersMessageV0(histogram=EMPTY_KLL, compact_theta=EMPTY_THETA, variance=variance_message)

    if distribution_metric is None:
        return msg_v0

    counts_metric: ColumnCountsMetric = col_prof.get_metric(ColumnCountsMetric.get_namespace())
    variance_message.count = counts_metric.n.value

    kll = distribution_metric.kll.value
    if not kll.is_empty():
        variance_message.mean = distribution_metric.mean.value
        variance_message.sum = distribution_metric.m2.value
        msg_v0 = NumbersMessageV0(histogram=kll.serialize(), compact_theta=EMPTY_THETA, variance=variance_message)

    return msg_v0


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


def _extract_schema_message_v0(col_prof: ColumnProfileView) -> SchemaMessageV0:
    metric: TypeCountersMetric = col_prof.get_metric(TypeCountersMetric.get_namespace())
    if metric is None:
        return SchemaMessageV0()

    type_counts: Dict[int, int] = {}
    type_counts[InferredType.INTEGRAL] = metric.integral.value
    type_counts[InferredType.BOOLEAN] = metric.boolean.value
    type_counts[InferredType.FRACTIONAL] = metric.fractional.value
    type_counts[InferredType.STRING] = metric.string.value
    type_counts[InferredType.UNKNOWN] = metric.object.value

    msg_v0 = SchemaMessageV0(
        typeCounts=type_counts,
    )

    return msg_v0


def _extract_col_counts(msg: ColumnMessageV0) -> ColumnCountsMetric:
    count_n = msg.counters.count
    count_null = msg.counters.null_count
    return ColumnCountsMetric(
        n=IntegralComponent(count_n or 0),
        null=IntegralComponent(count_null.value or 0),
        inf=IntegralComponent(0),
        nan=IntegralComponent(0),
    )


def _extract_counters_v0(col_prof: ColumnProfileView) -> CountersV0:
    counts_metric: ColumnCountsMetric = col_prof.get_metric(ColumnCountsMetric.get_namespace())
    count_options = dict(count=0)

    if counts_metric is None:
        return CountersV0(**dict(count=0))

    count_options = dict(count=counts_metric.n.value)

    return CountersV0(**count_options)


def _extract_dist_metric(msg: ColumnMessageV0) -> DistributionMetric:
    kll_bytes = msg.numbers.histogram
    floats_sk = None
    doubles_sk: Optional[ds.kll_doubles_sketch] = None
    # If this is a V1 serialized message it will be a double kll sketch.
    try:
        floats_sk = ds.kll_floats_sketch.deserialize(kll_bytes)
    except ValueError as e:
        logger.info(f"kll encountered old format which threw exception: {e}, attempting kll_doubles deserialization.")
    except RuntimeError as e:
        logger.warning(
            f"kll encountered runtime error in old format which threw exception: {e}, attempting kll_doubles deserialization."
        )
    if floats_sk is None:
        doubles_sk = ds.kll_doubles_sketch.deserialize(kll_bytes)
    else:
        doubles_sk = ds.kll_floats_sketch.float_to_doubles(floats_sk)

    dist_mean = msg.numbers.variance.mean
    dist_m2 = msg.numbers.variance.sum
    return DistributionMetric(
        kll=KllComponent(doubles_sk),
        mean=FractionalComponent(dist_mean),
        m2=FractionalComponent(dist_m2),
    )


def _extract_frequent_items_sketch_message_v0(col_prof: ColumnProfileView) -> FrequentItemsSketchMessageV0:
    frequent_items_metric: FrequentItemsMetric = col_prof.get_metric(FrequentItemsMetric.get_namespace())
    frequent_items_v0 = FrequentItemsSketchMessageV0(sketch=EMPTY_FI_SKETCH)
    if frequent_items_metric is None:
        return frequent_items_v0

    frequent_items_v0.sketch = frequent_items_metric.frequent_strings.value.serialize()
    frequent_items_v0.lg_max_k = _DEFAULT_V0_LG_MAX_K

    return frequent_items_v0


def _extract_hll_sketch_message_v0(col_prof) -> HllSketchMessageV0:
    cardinality_metric: CardinalityMetric = col_prof.get_metric(CardinalityMetric.get_namespace())
    hll_v0 = HllSketchMessageV0(sketch=EMPTY_HLL)
    if cardinality_metric is None:
        return hll_v0
    hll_sketch = cardinality_metric.hll.value
    if not hll_sketch.is_empty():
        hll_v0.sketch = hll_sketch.serialize_compact()
        hll_v0.lg_k = _DEFAULT_V0_LG_MAX_K

    return hll_v0


def v1_to_dataset_profile_message_v0(
    profile_view: DatasetProfileView,
    segment: Optional[Segment],
    partition: Optional[SegmentationPartition],
) -> DatasetProfileMessageV0:

    segment_message_tags: Optional[Dict[str, str]] = None
    segment_tags: Optional[List[SegmentTag]] = None
    segment_metadata: Optional[Dict[str, str]] = None

    if partition and partition.mapper and partition.mapper.col_names:
        segment_message_tags, segment_tags, segment_metadata = _generate_segment_tags_metadata(segment, partition)
    elif partition:
        raise NotImplementedError(
            f"Conversion of custom v1 segmented profiles to v0 not supported! please use column value segmentation, found tags: {segment_tags}"
        )

    properties_v0 = DatasetPropertiesV0(
        schema_major_version=1,  # https://github.com/whylabs/whylogs/blob/maintenance/0.7.x/src/whylogs/core/datasetprofile.py#L37-L38
        schema_minor_version=2,
        data_timestamp=to_utc_milliseconds(profile_view._dataset_timestamp),
        session_timestamp=to_utc_milliseconds(profile_view._creation_timestamp),
        metadata=segment_metadata,
        tags=segment_message_tags,
    )

    columns_v0: Dict[str, ColumnMessageV0] = {}
    columns = profile_view.get_columns()
    for col_name, col_prof in columns.items():
        columns_v0[col_name] = ColumnMessageV0(
            name=col_name,
            counters=_extract_counters_v0(col_prof),
            schema=_extract_schema_message_v0(col_prof),
            numbers=_extract_numbers_message_v0(col_prof),
            frequent_items=_extract_frequent_items_sketch_message_v0(col_prof),
            cardinality_tracker=_extract_hll_sketch_message_v0(col_prof),
        )
    metrics = profile_view.model_performance_metrics
    model_profile_message = metrics.to_protobuf() if metrics else None

    message = DatasetProfileMessageV0(
        properties=properties_v0,
        columns=columns_v0,
        modeProfile=model_profile_message,
    )
    return message
