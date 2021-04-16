from whylogs.core.statistics import SchemaTracker
from whylogs.proto import InferredType

Type = InferredType.Type


def multiple_track(tracker, counts: dict):
    for item, count in counts.items():
        for i in range(count):
            tracker.track(item)


def test_mainly_null_inferred_type_not_null():
    counts = {Type.INTEGRAL: 1, Type.NULL: 30}
    tracker = SchemaTracker()
    multiple_track(tracker, counts)
    inferred_type = tracker.infer_type()
    assert inferred_type.type == Type.INTEGRAL

    counts = {Type.INTEGRAL: 1, Type.STRING: 2, Type.NULL: 30}
    tracker = SchemaTracker()
    multiple_track(tracker, counts)
    inferred_type = tracker.infer_type()
    assert inferred_type.type != Type.NULL


def test_all_null_inferred_type_is_null():
    counts = {
        Type.NULL: 1,
    }
    tracker = SchemaTracker()
    multiple_track(tracker, counts)
    inferred_type = tracker.infer_type()
    assert inferred_type.type == Type.NULL


def test_track_nothing_should_return_unknown():
    tracker = SchemaTracker()
    inferred_type = tracker.infer_type()
    assert inferred_type.type == Type.UNKNOWN
    assert inferred_type.ratio == 0.0


def test_track_datatype_counts():
    type_counts = {
        Type.INTEGRAL: 2,
        Type.STRING: 2,
        Type.FRACTIONAL: 2,
        Type.BOOLEAN: 2,
        Type.UNKNOWN: 2,
    }
    tracker = SchemaTracker()
    multiple_track(tracker, type_counts)

    assert type_counts[Type.INTEGRAL] == tracker.get_count(Type.INTEGRAL)
    assert type_counts[Type.STRING] == tracker.get_count(Type.STRING)
    assert type_counts[Type.FRACTIONAL] == tracker.get_count(Type.FRACTIONAL)
    assert type_counts[Type.BOOLEAN] == tracker.get_count(Type.BOOLEAN)
    assert type_counts[Type.UNKNOWN] == tracker.get_count(Type.UNKNOWN)


def test_70percent_string():
    tracker = SchemaTracker()
    type_counts = {Type.INTEGRAL: 29, Type.STRING: 71}
    multiple_track(tracker, type_counts)
    assert tracker.infer_type().type == Type.STRING


def test_majority_float():
    tracker = SchemaTracker()
    type_counts = {
        Type.FRACTIONAL: 50,
        Type.STRING: 30,
        Type.UNKNOWN: 20,
    }
    multiple_track(tracker, type_counts)
    assert tracker.infer_type().type == Type.FRACTIONAL


def test_majority_int():
    tracker = SchemaTracker()
    type_counts = {
        Type.INTEGRAL: 50,
        Type.STRING: 30,
        Type.UNKNOWN: 20,
    }
    multiple_track(tracker, type_counts)
    assert tracker.infer_type().type == Type.INTEGRAL


def test_float_and_int():
    tracker = SchemaTracker()
    multiple_track(
        tracker,
        counts={
            Type.INTEGRAL: 50,
            Type.FRACTIONAL: 50,
            Type.STRING: 10,
        },
    )
    assert tracker.infer_type().type == Type.FRACTIONAL


def test_all_types_equal_coerced_to_string():
    tracker = SchemaTracker()
    multiple_track(
        tracker,
        counts={
            Type.INTEGRAL: 20,
            Type.FRACTIONAL: 29,
            Type.STRING: 50,
        },
    )
    assert tracker.infer_type().type == Type.STRING


def test_serialization_roundtrip():
    tracker = SchemaTracker()
    type_count = {
        Type.INTEGRAL: 10,
        Type.STRING: 100,
    }
    multiple_track(tracker, type_count)

    roundtrip = SchemaTracker.from_protobuf(tracker.to_protobuf())

    assert tracker.to_protobuf() == roundtrip.to_protobuf()
    assert roundtrip.get_count(Type.INTEGRAL) == 10
    assert roundtrip.get_count(Type.STRING) == 100


def test_summary():
    tracker = SchemaTracker()
    type_counts = {
        Type.INTEGRAL: 3,
        Type.STRING: 4,
        Type.FRACTIONAL: 5,
        Type.BOOLEAN: 6,
        Type.UNKNOWN: 1,
    }
    tracker = SchemaTracker()
    multiple_track(tracker, type_counts)

    summary = tracker.to_summary()
    c = summary.type_counts
    assert c["INTEGRAL"] == type_counts[Type.INTEGRAL]
    assert c["STRING"] == type_counts[Type.STRING]
    assert c["FRACTIONAL"] == type_counts[Type.FRACTIONAL]
    assert c["BOOLEAN"] == type_counts[Type.BOOLEAN]
    assert c["UNKNOWN"] == type_counts[Type.UNKNOWN]

    assert summary.inferred_type.type == tracker.infer_type().type


def test_merge_total_counts_match():
    x1 = SchemaTracker()
    multiple_track(
        x1, {Type.INTEGRAL: 10, Type.FRACTIONAL: 10, Type.BOOLEAN: 10, Type.UNKNOWN: 10}
    )

    x2 = SchemaTracker()
    multiple_track(
        x2, {Type.INTEGRAL: 20, Type.FRACTIONAL: 20, Type.BOOLEAN: 20, Type.UNKNOWN: 20}
    )

    merged = x1.merge(x2)
    assert merged.get_count(Type.INTEGRAL) == 30
    assert merged.get_count(Type.FRACTIONAL) == 30
    assert merged.get_count(Type.BOOLEAN) == 30
    assert merged.get_count(Type.UNKNOWN) == 30

    # Make sure we can serialize round trip
    SchemaTracker.from_protobuf(merged.to_protobuf())
