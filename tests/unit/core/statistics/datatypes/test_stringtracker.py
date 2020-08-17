from whylabs.logs.core.statistics.datatypes import StringTracker
from whylabs.logs.util.protobuf import message_to_dict
import datasketches
import datetime


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
    assert [("one", 3, 3, 3)] == x.items.get_frequent_items(
        datasketches.frequent_items_error_type.NO_FALSE_NEGATIVES, 2
    )

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
    assert (
        x.theta_sketch.get_result().get_estimate()
        == x2.theta_sketch.get_result().get_estimate()
    )


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
    }
    expected_items = pd.DataFrame(expected["frequent"]["items"]).sort_values(
        ["value", "estimate"]
    )
    expected["frequent"].pop("items")

    actual = message_to_dict(x.to_summary())
    actual_items = pd.DataFrame(actual["frequent"]["items"]).sort_values(
        ["value", "estimate"]
    )
    actual["frequent"].pop("items")

    assert expected == actual
    pd.testing.assert_frame_equal(
        actual_items.reset_index(drop=True).sort_index(axis=1),
        expected_items.reset_index(drop=True).sort_index(axis=1),
    )
