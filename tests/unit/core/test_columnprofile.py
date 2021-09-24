import numpy as np
import pandas as pd
import pytest

from whylogs.core import ColumnProfile
from whylogs.core.statistics.hllsketch import HllSketch
from whylogs.proto import InferredType
from whylogs.util.dsketch import FrequentItemsSketch
from whylogs.util.protobuf import message_to_dict
from whylogs.v2.core import ColumnProfile as ColumnProfileV2

test_column_types = [ColumnProfile, ColumnProfileV2]


@pytest.mark.parametrize("column_type", test_column_types)
def test_all_numeric_types_get_tracked_by_number_tracker(column_type):
    all_values = [
        [1.0, 2.0, 3.0],
        [1, 2, 3],
        np.arange(4),
        np.linspace(1, 2, 5),
        pd.Series(np.arange(3)),
        np.zeros(3, dtype=np.int32),
        np.zeros(3, dtype=np.int16),
    ]
    for values in all_values:
        c = column_type("test")
        for v in values:
            c.track(v)
        if hasattr(c, "number_tracker"):
            assert c.number_tracker.count == len(values)
        else:
            c.get_tracker("NumberTracker").count == len(values)


@pytest.mark.parametrize("column_type", test_column_types)
def test_all_nulls_inferred_type_null(column_type):
    import numpy as np

    from whylogs.proto import InferredType

    Type = InferredType.Type
    c = column_type("col")
    data = [None, np.nan, None] * 3
    for val in data:
        c.track(val)
    summary = c.to_summary()
    if hasattr(summary, "counters"):
        assert summary.counters.null_count.value == 9
        assert summary.schema.inferred_type.type == Type.NULL
    else:
        summary.trackers["Counters"].n == 9
        summary.trackers["SchemaTracker"].inferred_type.type == Type.NULL


@pytest.mark.parametrize("column_type", test_column_types)
def test_mostly_nulls_inferred_type_not_null(column_type):
    import numpy as np

    from whylogs.proto import InferredType

    Type = InferredType.Type
    c = column_type("col")
    data = [None, np.nan, None] * 3 + ["not a null val!"]
    for val in data:
        c.track(val)
    summary = c.to_summary()
    if hasattr(summary, "schema"):
        assert summary.schema.inferred_type.type != Type.NULL
    else:
        summary.trackers["SchemaTracker"].inferred_type.type != Type.NULL


@pytest.mark.parametrize("column_type", test_column_types)
def test_frequent_items_do_not_track_nulls(column_type):
    import numpy as np

    data = [None, np.nan, None]
    c = column_type("col")
    for val in data:
        c.track(val)
    if hasattr(c, "frequent_items"):
        assert c.frequent_items.to_summary() is None
        assert c.frequent_items.is_empty()
        assert c.cardinality_tracker.is_empty()
        assert c.cardinality_tracker.to_summary() is None
    else:
        c.get_tracker("FrequentItemsSketch").to_summary() is None
        c.get_tracker("FrequentItemsSketch").is_empty()
        unique_count_summary = c._unique_count_summary()
        unique_count_summary is None


@pytest.mark.parametrize("column_type", test_column_types)
def test_track(column_type):
    c = column_type("col")
    data = [1, 2, 3, "string 1", "string 2", "3", 4.0, "3.95", "3.95st", None, True]
    for val in data:
        c.track(val)
    if hasattr(c, "number_tracker"):
        nt = c.number_tracker
        assert nt.floats.count == 6
        assert nt.ints.count == 0
        assert nt.floats.min == 1.0
        assert nt.floats.max == 4.0
    else:
        nt = c.get_tracker("NumberTracker")
        assert nt.floats.count == 4  # The V2 tracker doesn't parse strings into floats
        assert nt.ints.count == 0
        assert nt.floats.min == 1.0
        assert nt.floats.max == 4.0

    if hasattr(c, "counters"):
        assert c.counters.count == len(data)
        assert c.counters.true_count == 1
        assert c.schema_tracker.get_count(InferredType.Type.NULL) == 1
    else:
        assert c.get_tracker("Counter").count == len(data)
        assert c.get_tracker("SchemaTracker").get_count(InferredType.Type.NULL) == 1


@pytest.mark.parametrize("column_type", test_column_types)
def test_protobuf(column_type):
    c = column_type("col")
    for val in [1, 2, 3]:
        c.track(val)
    is_v0 = hasattr(c, "number_tracker")
    profile_type = ColumnProfile if is_v0 else ColumnProfileV2
    msg = c.to_protobuf()
    c1 = profile_type.from_protobuf(msg)
    assert c1.column_name == c.column_name == "col"
    if is_v0:
        assert hasattr(c1, "number_tracker")
        assert hasattr(c1, "string_tracker")
        assert c1.string_tracker.length is not None
        assert c1.string_tracker.length.count == 0
        assert len(c1.string_tracker.char_pos_tracker.character_list) == 56
    else:
        string_tracker = c1.get_tracker("StringTracker")
        assert string_tracker is not None
        assert string_tracker.length is not None
        assert string_tracker.length.count == 0
    c1.to_protobuf()


def test_summary():
    c = ColumnProfile("col")
    for n in [1, 2, 3]:
        c.track(n)
    summary = c.to_summary()
    actual_val = message_to_dict(summary)
    print(actual_val)
    expected_val = {
        "counters": {
            "count": "3",
        },
        "schema": {
            "inferredType": {"type": "INTEGRAL", "ratio": 1.0},
            "typeCounts": {"INTEGRAL": "3"},
        },
        "numberSummary": {
            "count": "3",
            "min": 1.0,
            "max": 3.0,
            "mean": 2.0,
            "stddev": 1.0,
            "isDiscrete": False,
            "histogram": {
                "start": 1.0,
                "end": 3.0000003,
                "counts": ["3"],
                "max": 3.0,
                "min": 1.0,
                "bins": [1.0, 3.0000003],
                "n": "3",
                "width": 0.0,
            },
            "quantiles": {
                "quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
                "quantileValues": [1.0, 1.0, 1.0, 1.0, 2.0, 3.0, 3.0, 3.0, 3.0],
            },
            "uniqueCount": {"estimate": 3.0, "upper": 3.0, "lower": 3.0},
        },
    }

    # Top-level unique count needs to be approximately equal
    expected_unique = {
        "estimate": 3.000000014901161,
        "lower": 3.0,
        "upper": 3.0001498026537594,
    }
    actual_unique = actual_val.pop("uniqueCount")
    assert actual_unique == pytest.approx(expected_unique, 0.0001)

    # Cannot do a straightforward frequentItems count since order is ambiguous
    actual_freq = actual_val.pop("frequentItems")
    assert set(actual_freq.keys()) == {"items"}
    expected = [("1", "1"), ("2", "1"), ("3", "1")]
    assert len(actual_freq["items"]) == len(expected)
    counts = []
    for v in actual_freq["items"]:
        counts.append((v["jsonValue"], v["estimate"]))
    assert set(counts) == set(expected)

    # Compare the messages, excluding the frequent numbers counters
    assert actual_val == expected_val


def test_summary_v2():
    c = ColumnProfileV2("col")
    for n in [1, 2, 3]:
        c.track(n)
    summary = c.to_summary()
    actual_val = message_to_dict(summary)

    # Top-level unique count needs to be approximately equal
    expected_unique = {
        "estimate": 3.000000014901161,
        "lower": 3.0,
        "upper": 3.0001498026537594,
    }
    actual_unique = actual_val.get("trackers").get("HllSketch").get("uniqueCount")
    assert actual_unique == pytest.approx(expected_unique, 0.0001)

    # Cannot do a straightforward frequentItems count since order is ambiguous
    actual_freq = actual_val.get("trackers").get("FrequentItemsSketch").get("frequentItems")

    assert set(actual_freq.keys()) == {"items"}
    expected = [("1", "1"), ("2", "1"), ("3", "1")]
    assert len(actual_freq["items"]) == len(expected)
    counts = []
    for v in actual_freq["items"]:
        counts.append((v["jsonValue"], v["estimate"]))
    assert set(counts) == set(expected)


@pytest.mark.parametrize("column_type", test_column_types)
def test_merge(column_type):
    col = column_type("test")
    vals = [1, 1.0, "string", True, False, None]
    for v in vals:
        col.track(v)

    merged = col.merge(col)
    if hasattr(merged, "counters"):
        assert merged.counters.count == 12
        assert merged.schema_tracker.get_count(InferredType.Type.NULL) == 2
        assert merged.counters.true_count == 4
        assert merged.number_tracker.ints.count == 0
        assert merged.number_tracker.floats.count == 4
        assert merged.string_tracker.count == 2
    else:
        assert merged.get_tracker("Counter").count == 12
        assert merged.get_tracker("SchemaTracker").get_count(InferredType.Type.NULL) == 2
        assert merged.get_tracker("SchemaTracker").get_count(InferredType.Type.BOOLEAN) == 4
        assert merged.get_tracker("NumberTracker").ints.count == 0
        assert merged.get_tracker("NumberTracker").floats.count == 4
        assert merged.get_tracker("StringTracker").count == 2


@pytest.mark.parametrize("column_type", test_column_types)
def test_fallback_number_counter(column_type):
    col = column_type("test")
    vals = [1, 1.0, 2, 3, 4, 5, 6, 6.0, "text"]
    for v in vals:
        col.track(v)
    col.cardinality_tracker = HllSketch()

    summary = col.to_summary()
    if hasattr(summary, "unique_count"):
        assert summary.unique_count.estimate == summary.number_summary.unique_count.estimate
    else:
        assert summary.trackers["HllSketch"].unique_count.estimate == pytest.approx(len(vals), 0.01)


@pytest.mark.parametrize("column_type", test_column_types)
def test_fallback_string_counter(column_type):
    col = column_type("test")
    vals = ["a", "b", "c", "d", "e", "f", 1.0, 2.0]
    expected_string_vals = [s for s in filter(lambda v: isinstance(v, str), vals)]
    for v in vals:
        col.track(v)
    col.cardinality_tracker = HllSketch()

    summary = col.to_summary()
    if hasattr(summary, "unique_count"):
        assert summary.unique_count.estimate == summary.string_summary.unique_count.estimate
    else:
        assert summary.trackers["StringTracker"].strings.unique_count.estimate == pytest.approx(len(expected_string_vals), 0.01)


@pytest.mark.parametrize("column_type", test_column_types)
def test_fallback_fallbacks_to_number_counter(column_type):
    col = column_type("test")
    vals = ["a", "b", 1.0, 2.0, 10.0, 30.0, 15.0]
    for v in vals:
        col.track(v)
    col.cardinality_tracker = HllSketch()

    summary = col.to_summary()
    if hasattr(summary, "unique_count"):
        assert summary.unique_count.estimate == summary.number_summary.unique_count.estimate
    else:
        unique_estimate = summary.trackers["NumberTracker"].numbers.unique_count.estimate
        print(f"Unique estimate is {unique_estimate}")
        assert summary.trackers["NumberTracker"].numbers.unique_count.estimate == pytest.approx(len(vals), 1.0)


@pytest.mark.parametrize("column_type", test_column_types)
def test_copy_counters_null_count_in_schema_tracker(column_type):
    col = column_type("test")
    vals = ["a", "b", None, "d", pd.NA, "f", 1.0, 2.0]
    for v in vals:
        col.track(v)

    expected_nulls = [s for s in filter(lambda v: pd.isnull(v), vals)]
    if hasattr(col, "schema_tracker"):
        assert col.schema_tracker.get_count(InferredType.Type.NULL) == len(expected_nulls)
    else:
        assert col.get_tracker("SchemaTracker").get_count(InferredType.Type.NULL) == len(expected_nulls)

    # ensuring we can still access the value in summary mode
    summary = col.to_summary()
    if hasattr(summary, "counters"):
        assert summary.counters.null_count.value == len(expected_nulls)
    else:
        assert summary.trackers["Counter"].n == len(vals)
        assert summary.trackers["SchemaTracker"].schema.type_counts["NULL"] == len(expected_nulls)

    # Mimic a legal protobuf with null_count set
    msg = col.to_protobuf()
    null_count = 314
    v0_expected_null_count = null_count + len(expected_nulls)  # counters.null_count is added to the observed type_counts in v0
    if hasattr(msg, "counters"):
        msg.counters.null_count.value = null_count
    else:
        msg.trackers["SchemaTracker"].schema.typeCounts[InferredType.Type.NULL] = null_count

    roundtrip = column_type.from_protobuf(msg)
    if hasattr(roundtrip, "schema_tracker"):
        assert roundtrip.schema_tracker.get_count(InferredType.Type.NULL) == v0_expected_null_count
    else:
        assert roundtrip.get_tracker("SchemaTracker").get_count(InferredType.Type.NULL) == null_count


def test_custom_no_tracker_config():
    test_column_name = "test_col1"
    col = ColumnProfileV2(test_column_name, trackers=[])
    vals = ["a", "b", None, "d", pd.NA, "f", 1.0, 2.0]
    for v in vals:
        col.track(v)

    expected_nulls = [s for s in filter(lambda v: pd.isnull(v), vals)]
    assert col.get_tracker("SchemaTracker") is None

    # ensuring we can still access the value in summary mode
    summary = col.to_summary()
    print(summary)
    assert summary.name == test_column_name

    # test round trip
    msg = col.to_protobuf()
    print(msg)
    assert msg.name == test_column_name

    roundtrip = ColumnProfileV2.from_protobuf(msg)
    print(roundtrip)
    assert roundtrip.column_name == test_column_name
    print(roundtrip.get_tracker_names())
    assert len(roundtrip.get_tracker_names()) == 0
