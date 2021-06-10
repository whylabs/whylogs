import datasketches
import pytest

from whylogs.core.statistics import StringTracker
from whylogs.util.protobuf import message_to_dict


def test_character_pos_tracker():

    x = StringTracker()

    data = ["abc abc", "93341-1", "912254", None]
    no_nulls = [a for a in data if a is not None]
    count = len(no_nulls)
    n_unique = len(set(no_nulls))

    for record in data:
        x.update(record)

    assert x.items.get_num_active_items() == n_unique
    assert x.items.get_total_weight() == count

    assert pytest.approx(x.length.ints.mean(), 0.001) == 6.666

    assert pytest.approx(x.token_length.ints.mean(), 0.001) == 1.333

    assert x.theta_sketch.get_result().get_estimate() == float(n_unique)
    assert x.count == count

    assert x.char_pos_tracker is not None


def test_tracking():
    x = StringTracker()
    data = ["one", "two", "three", "one", "one", "One", "six", None, None]
    no_nulls = [a for a in data if a is not None]
    count = len(no_nulls)
    n_unique = len(set(no_nulls))

    for record in data:
        x.update(record)

    assert x.items.get_num_active_items() == n_unique
    assert x.items.get_total_weight() == count
    assert [("one", 3, 3, 3)] == x.items.get_frequent_items(datasketches.frequent_items_error_type.NO_FALSE_NEGATIVES, 2)

    assert x.theta_sketch.get_result().get_estimate() == float(n_unique)
    assert x.count == count


def test_protobuf():
    x = StringTracker()
    data = ["one", "two", "three", "one", "one", "One", "six", None, None]
    for record in data:
        x.update(record)
    x2 = StringTracker.from_protobuf(x.to_protobuf())
    assert x.count == x2.count
    assert x.items.get_total_weight() == x2.items.get_total_weight()
    assert x.theta_sketch.get_result().get_estimate() == x2.theta_sketch.get_result().get_estimate()


def test_summary():
    import pandas as pd

    x = StringTracker()
    data = ["one", "two", "three", "one", "one", "One", "six", None, None]
    for record in data:
        x.update(record)
    # Check the full output.  NOTE: the order of the "items" below should
    # really be arbitrary
    expected = {
        "uniqueCount": {"estimate": 5.0, "upper": 5.0, "lower": 5.0},
        "frequent": {
            "items": [
                {"value": "one", "estimate": 3.0},
                {"value": "three", "estimate": 1.0},
                {"value": "six", "estimate": 1.0},
                {"value": "One", "estimate": 1.0},
                {"value": "two", "estimate": 1.0},
            ]
        },
        "length": {
            "count": "7",
            "min": 3.0,
            "max": 5.0,
            "mean": 3.2857142857142856,
            "stddev": 0.7559289460184544,
            "histogram": {
                "start": 3.0,
                "end": 5.0000005,
                "counts": ["6", "1"],
                "max": 5.0,
                "min": 3.0,
                "bins": [3.0, 4.000000249999999, 5.0000005],
                "n": "7",
                "width": 0.0,
            },
            "uniqueCount": {"estimate": 2.0, "upper": 2.0, "lower": 2.0},
            "quantiles": {"quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0], "quantileValues": [3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0, 5.0, 5.0]},
            "frequentNumbers": {"longs": [{"estimate": "6", "value": "3", "rank": 0}, {"estimate": "1", "value": "5", "rank": 1}], "doubles": []},
            "isDiscrete": False,
        },
        "tokenLength": {
            "count": "7",
            "min": 1.0,
            "max": 1.0,
            "mean": 1.0,
            "histogram": {"start": 1.0, "end": 1.0000001, "counts": ["7"], "max": 1.0, "min": 1.0, "bins": [1.0, 1.0000001], "n": "7", "width": 0.0},
            "uniqueCount": {"estimate": 1.0, "upper": 1.0, "lower": 1.0},
            "quantiles": {"quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0], "quantileValues": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]},
            "frequentNumbers": {"longs": [{"estimate": "7", "value": "1", "rank": 0}], "doubles": []},
            "stddev": 0.0,
            "isDiscrete": False,
        },
    }
    expected_items = pd.DataFrame(expected["frequent"]["items"]).sort_values(["value", "estimate"])
    expected["frequent"].pop("items")

    actual = message_to_dict(x.to_summary())
    actual_items = pd.DataFrame(actual["frequent"]["items"]).sort_values(["value", "estimate"])
    actual["frequent"].pop("items")

    print(actual)
    assert expected == actual
    pd.testing.assert_frame_equal(
        actual_items.reset_index(drop=True).sort_index(axis=1),
        expected_items.reset_index(drop=True).sort_index(axis=1),
    )
