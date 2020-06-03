"""
"""
import pytest
from whylabs.logs.core.statistics import NumberTracker


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
