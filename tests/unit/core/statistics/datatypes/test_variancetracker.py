"""
"""
import pytest

from whylogs.core.statistics.datatypes import VarianceTracker


def test_update():
    vals = [1.0, 2.0, 3]
    tracker = VarianceTracker()
    for v in vals:
        tracker.update(v)

    assert tracker.mean == pytest.approx(2.0, 1e-6)
    assert tracker.count == len(vals)
    # Note: this is a population variance estimate (normalized by N-1)
    assert tracker.variance() == pytest.approx(1.0, 1e-6)


def test_merge_simple_trackers():
    x1 = VarianceTracker()
    x1.update(1.0)

    x2 = VarianceTracker()
    x2.update(2.0)
    x2.update(3.0)

    merged = x1.merge(x2)
    assert merged.mean == pytest.approx(2.0, 1e-4)
    assert merged.variance() == pytest.approx(1.0, 1e-4)
    assert merged.count == 3


def test_merge_with_empty_tracker():
    x1 = VarianceTracker()
    vals = list(range(10))
    for v in vals:
        x1.update(v)

    var = x1.variance()
    mean = x1.mean
    count = x1.count

    x2 = x1.merge(VarianceTracker())

    assert var == x2.variance()
    assert mean == x2.mean
    assert count == x2.count

    assert x1.variance() == x2.variance()
    assert x1.mean == x2.mean
    assert x1.count == x2.count


def test_empty_merge_full():
    x1 = VarianceTracker()
    vals = list(range(10))
    for v in vals:
        x1.update(v)

    var = x1.variance()
    mean = x1.mean
    count = x1.count

    x2 = VarianceTracker().merge(x1)

    assert var == x2.variance()
    assert mean == x2.mean
    assert count == x2.count

    assert x1.variance() == x2.variance()
    assert x1.mean == x2.mean
    assert x1.count == x2.count


def test_merge_bigger():
    x1 = VarianceTracker()
    for i in range(10):
        x1.update(i)

    assert x1.variance() == pytest.approx(9.1667, 1e-4)
    assert x1.count == 10
    assert x1.mean == pytest.approx(4.5, 1e-7)

    merged = x1.merge(x1.copy())
    assert merged.variance() == pytest.approx(8.684, 1e-4)
    assert merged.count == 20
    assert merged.mean == pytest.approx(4.5, 1e-7)


def test_copy():
    x1 = VarianceTracker()
    for i in range(10):
        x1.update(i)

    var = x1.variance()
    count = x1.count
    mean = x1.mean

    x2 = x1.copy()
    assert x2.variance() == var
    assert x2.count == count
    assert x2.mean == mean

    for i in range(3):
        x1.update(i)

    assert x1.variance() != var
    assert x1.count == count + 3
    assert x1.mean != mean

    assert x2.variance() == var
    assert x2.mean == mean
    assert x2.count == count


def test_protobuf_roundtrip_matches():
    x1 = VarianceTracker()
    for i in range(10):
        x1.update(i)

    proto = x1.to_protobuf()
    roundtrip = VarianceTracker.from_protobuf(proto)

    assert x1.count == roundtrip.count
    assert x1.variance() == roundtrip.variance()
    assert x1.mean == roundtrip.mean
