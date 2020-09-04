import pytest
from testutil import compare_frequent_items

from whylogs.core.statistics import NumberTracker


def test_count_is_correct():
    x = NumberTracker()
    assert x.count == 0
    x.track(None)
    assert x.count == 0
    for val in [1, 2, 3]:
        x.track(val)
    assert x.count == 3
    for val in [1.0, 2.0]:
        x.track(val)
    assert x.count == 5


def test_int_value_should_not_increase_float_count():
    x = NumberTracker()
    for v in [10, 11, 12]:
        x.track(v)

    assert x.ints.count == 3
    assert x.floats.count == 0
    assert x.variance.stddev() == pytest.approx(1.0, 1e-3)

    assert x.theta_sketch.get_result().get_estimate() == pytest.approx(3, 1e-4)

    hist = x.histogram
    assert hist.get_n() == 3
    assert hist.get_max_value() == pytest.approx(12, 1e-4)
    assert hist.get_min_value() == pytest.approx(10, 1e-4)


def test_float_after_int_resets_int_tracker():
    x = NumberTracker()

    x.track(10)
    x.track(11)
    assert x.ints.count == 2
    assert x.floats.count == 0

    x.track(12.0)

    assert x.ints.count == 0
    assert x.floats.count == 3
    assert x.variance.stddev() == pytest.approx(1.0, 1e-3)

    assert x.histogram.get_n() == 3
    assert x.theta_sketch.get_result().get_estimate() == pytest.approx(3, 1e-4)
    assert x.histogram.get_max_value() == pytest.approx(12, 1e-4)
    assert x.histogram.get_min_value() == pytest.approx(10, 1e-4)


def test_empty_merge_succeeds():
    x1 = NumberTracker()
    x2 = NumberTracker()
    x3 = x1.merge(x2)
    assert isinstance(x3, NumberTracker)


def test_merge():
    x = NumberTracker()
    for v in [10, 11, 13]:
        x.track(v)

    merged = x.merge(x)

    assert merged.ints.count == 6
    assert merged.floats.count == 0
    assert merged.histogram.get_n() == 6
    assert merged.histogram.get_max_value() == 13.0
    assert merged.histogram.get_min_value() == 10.0
    expected_freq = [
        (10, 2, 2, 2),
        (11, 2, 2, 2),
        (13, 2, 2, 2),
    ]
    compare_frequent_items(expected_freq, merged.frequent_numbers.get_frequent_items())

    msg = merged.to_protobuf()
    NumberTracker.from_protobuf(msg)


def test_protobuf_roundtrip():
    x0 = NumberTracker()
    for v in [10, 11, 13]:
        x0.track(v)

    msg = x0.to_protobuf()
    roundtrip = NumberTracker.from_protobuf(msg)

    assert x0.ints.count == roundtrip.ints.count
    assert x0.floats.count == roundtrip.floats.count
    assert x0.histogram.get_n() == roundtrip.histogram.get_n()
    assert x0.histogram.get_min_value() == roundtrip.histogram.get_min_value()
    assert x0.histogram.get_max_value() == roundtrip.histogram.get_max_value()


def test_high_cardinality_not_discrete():
    vals = 3 * [1, 2, 3] + [4.0, 6.0, 9.0, 9.0]
    x = NumberTracker()
    for v in vals:
        x.track(v)
    summary = x.to_summary()
    assert not summary.is_discrete


def test_one_value_not_discrete():
    x = NumberTracker()
    x.track(1)
    assert not x.to_summary().is_discrete


def test_low_cardinality_is_discrete():
    vals = 3 * [1, 2, 3] + [4.0, 6.0, 9.0, 9.0]
    vals = vals * 10
    x = NumberTracker()
    for v in vals:
        x.track(v)
    summary = x.to_summary()
    assert summary.is_discrete


def test_track_floats_ints_unique_in_cardinality_estimate():
    vals = [1, 2, 3, 4]
    x = NumberTracker()
    for val in vals:
        x.track(val)

    assert x.to_summary().unique_count.estimate == 4

    for val in vals:
        x.track(float(val))

    assert x.to_summary().unique_count.estimate == 8
