"""
"""
from whylabs.logs.core.statistics import thetasketch
import os

MYDIR = os.path.realpath(os.path.dirname(__file__))
DATA_DIR = os.path.join(MYDIR, "data")


def test_deserialize_returns_theta_sketch():
    with open(os.path.join(DATA_DIR, "numeric_theta_sketch.bin"), "rb") as fp:
        theta_bytes = fp.read()

    theta = thetasketch.ThetaSketch.deserialize(theta_bytes)
    assert isinstance(theta, thetasketch.ThetaSketch)

    with open(os.path.join(DATA_DIR, "string_theta_sketch.bin"), "rb") as fp:
        theta_bytes = fp.read()

    theta = thetasketch.ThetaSketch.deserialize(theta_bytes)
    assert isinstance(theta, thetasketch.ThetaSketch)


def test_update():
    theta = thetasketch.ThetaSketch()
    vals = [1, 2, 3, 1, 2, 3]
    for val in vals:
        theta.update(val)

    compact = theta.get_result()
    assert compact.get_estimate() == len(set(vals))


def test_merge_equivalent_to_single_run():
    vals1 = [1, 2, 3, 1, 2, 3]
    vals2 = [1, 2, 3, 4, 5]
    all_vals = vals1 + vals2

    theta1 = thetasketch.ThetaSketch()
    for v in vals1:
        theta1.update(v)

    theta2 = thetasketch.ThetaSketch()
    for v in vals2:
        theta2.update(v)

    theta3 = thetasketch.ThetaSketch()
    for v in all_vals:
        theta3.update(v)

    merged_2_into_1 = theta1.merge(theta2).get_result()
    merged_1_into_2 = theta2.merge(theta1).get_result()
    compact = theta3.get_result()

    assert merged_1_into_2.get_estimate() == merged_2_into_1.get_estimate()
    assert merged_1_into_2.get_estimate() == compact.get_estimate()

    assert merged_1_into_2.get_lower_bound(1) == merged_2_into_1.get_lower_bound(1)
    assert merged_1_into_2.get_lower_bound(1) == compact.get_lower_bound(1)

    assert merged_1_into_2.get_upper_bound(1) == merged_2_into_1.get_upper_bound(1)
    assert merged_1_into_2.get_upper_bound(1) == compact.get_upper_bound(1)


def test_serialize_roundtrip():
    theta = thetasketch.ThetaSketch()
    vals = [1, 2, 3, 1, 2, 3]
    for v in vals:
        theta.update(v)

    msg = theta.serialize()
    theta2 = thetasketch.ThetaSketch.deserialize(msg)

    compact1 = theta.get_result()
    compact2 = theta2.get_result()

    assert compact1.get_estimate() == compact2.get_estimate()
    assert compact1.get_upper_bound(1) == compact2.get_upper_bound(1)
    assert compact1.get_lower_bound(1) == compact2.get_lower_bound(1)
    assert compact1.get_num_retained() == compact2.get_num_retained()


def test_summary():
    theta = thetasketch.ThetaSketch()
    vals = 3 * [1, 2, 3] + [4.0, 6.0, 9.0, 9.0]
    for v in vals:
        theta.update(v)

    n = len(set(vals))
    summary = theta.to_summary()
    assert summary.estimate == n
    assert summary.upper == n
    assert summary.lower == n
