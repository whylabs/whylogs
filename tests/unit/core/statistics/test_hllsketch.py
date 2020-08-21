"""
"""
from datetime import datetime
from enum import Enum

import pytest
import pytz

from whylogs.core.statistics import hllsketch


def test_serialize_then_merge_does_not_fail():
    """
    There is a bizarre circumstance when serializing a dataksetches HLL sketch
    using sketch.serialize_updatable() which results in an occasional
    RuntimeError after deserialization and trying to update a union with that
    sketch.

    This bug does not _always_ creep up, therefore this test must be run
    many times.

    It appears to work when using serialize_compact() instead.

    This also only seems to occur with numpy datatypes, e.g. np.float, rather
    than with python floats.
    """
    import numpy as np

    vals = np.array(
        [
            21.0,
            15.0,
            14.0,
            23.0,
            24.0,
            2.0,
            26.0,
            31.0,
            26.0,
            30.0,
            17.0,
            36.0,
            18.0,
            28.0,
            58.0,
            32.0,
            10.0,
            19.0,
            15.0,
            8.0,
            31.0,
        ]
    )

    for attempt in range(50):
        # A larger lg_k seems to increase the odds of failure
        hll = hllsketch.HllSketch(20)
        for v in vals:
            hll.update(v)
        msg = hll.to_protobuf()
        msg_str = msg.SerializeToString()
        msg1 = hllsketch.HllSketchMessage.FromString(msg_str)
        hll2 = hllsketch.HllSketch.from_protobuf(msg1)
        hll2.merge(hll2)


def test_protobuf_then_merge():
    hll = hllsketch.HllSketch()
    vals = [1, 2, 3]
    hll2 = hllsketch.HllSketch.from_protobuf(hll.to_protobuf())
    hll2.merge(hll2)


def test_datetime_tracking():
    hll = hllsketch.HllSketch()
    vals = [
        datetime(2020, 2, 3),
        datetime(2020, 2, 3),
        datetime(2020, 2, 3, tzinfo=pytz.UTC),
        datetime(2020, 2, 3, 21, 28, 31),
        datetime.utcnow(),
    ]
    for v in vals:
        hll.update(v)

    assert hll.get_estimate() == pytest.approx(4, 1e-5)


def test_enum_tracking():
    class X(Enum):
        A = 1
        B = 2
        C = 3
        D = 4

    vals = [X.A, X.B, X.C, X.D] + [1, 2, 3, 4]
    hll = hllsketch.HllSketch()
    for v in vals:
        hll.update(v)

    assert hll.get_estimate() == pytest.approx(4, 1e-5)


def test_floats_ints_different():
    int_vals = [1, 2, 3] * 2
    float_vals = [1.0, 2.0, 3.0] * 2
    hll = hllsketch.HllSketch()
    for v in int_vals + float_vals:
        hll.update(v)

    assert hll.get_estimate() == pytest.approx(6, 1e-5)


def test_update():
    hll = hllsketch.HllSketch()
    vals = [1, 2, 3] * 2
    for v in vals:
        hll.update(v)

    assert hll.get_estimate() == pytest.approx(3, 1e-5)
    assert hll.get_upper_bound() == pytest.approx(3, 1e-3)
    assert hll.get_lower_bound() == pytest.approx(3, 1e-3)


def test_merge_equivalent_to_single_run():
    vals1 = [1, 2, 3, 1, 2, 3]
    vals2 = [1, 2, 3, 4, 5]
    all_vals = vals1 + vals2

    hll1 = hllsketch.HllSketch()
    for v in vals1:
        hll1.update(v)

    hll2 = hllsketch.HllSketch()
    for v in vals2:
        hll2.update(v)

    hll3 = hllsketch.HllSketch()
    for v in all_vals:
        hll3.update(v)

    merged_2_into_1 = hll1.merge(hll2)
    merged_1_into_2 = hll2.merge(hll1)

    assert merged_1_into_2.get_estimate() == merged_2_into_1.get_estimate()
    assert merged_1_into_2.get_estimate() == hll3.get_estimate()

    assert merged_1_into_2.get_lower_bound(1) == merged_2_into_1.get_lower_bound(1)
    assert merged_1_into_2.get_lower_bound(1) == hll3.get_lower_bound(1)

    assert merged_1_into_2.get_upper_bound(1) == merged_2_into_1.get_upper_bound(1)
    assert merged_1_into_2.get_upper_bound(1) == hll3.get_upper_bound(1)


def test_protobuf_roundtrip():
    hll1 = hllsketch.HllSketch()
    vals = [1, 2, 3, 1, 2, 3, "a", "a", "b", 2.0, 8.0, 9.0]
    for v in vals:
        hll1.update(v)

    msg = hll1.to_protobuf()
    hll2 = hllsketch.HllSketch.from_protobuf(msg)

    assert hll1.get_estimate() == hll2.get_estimate()
    assert hll1.get_upper_bound() == hll2.get_upper_bound()
    assert hll1.get_lower_bound() == hll2.get_lower_bound()


def test_summary():
    hll = hllsketch.HllSketch()
    vals = 3 * [1, 2, 3] + [4.0, 6.0, 9.0, 9.0] + ["a", "a", "dkfjadlf", "c"]
    for v in vals:
        hll.update(v)

    n = 3 + 3 + 3
    summary = hll.to_summary()
    assert summary.estimate == pytest.approx(n, 1e-5)
    assert summary.upper == pytest.approx(n, 1e-4)
    assert summary.lower == pytest.approx(n, 1e-4)


def test_empty_sketch_summary_returns_none():
    sketch = hllsketch.HllSketch()
    assert sketch.to_summary() is None
