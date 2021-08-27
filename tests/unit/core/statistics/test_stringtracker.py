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
    assert x.length.count == 3
    assert x.char_pos_tracker.char_pos_map["a"].histogram.get_min_value() == 0
    assert x.char_pos_tracker.char_pos_map["a"].count == 2
    assert x.char_pos_tracker.char_pos_map["-"].histogram.get_min_value() == 5


def test_unicode_character_pos():
    x = StringTracker()
    data = ["👷‍♀️ 🤺"]
    for record in data:
        x.update(record, character_list="🤺")

    assert x.char_pos_tracker.char_pos_map["NITL"].count == 5
    assert x.char_pos_tracker.char_pos_map["🤺"].count == 1
    assert x.char_pos_tracker.char_pos_map["🤺"].histogram.get_min_value() == 5


def test_merge_character_pos():
    x = StringTracker()
    y = StringTracker()
    data = ["abc abc", "93341-1", "912254", "bac tralalala"]
    data_2 = ["geometric inference ", "93341-1", "912254", "bac tralalala", "😀 this is a sale! a ❄️ sale!", "this is a long sentence that ends in an A", None]
    for record in data:
        x.update(record)
    for record in data_2:
        y.update(record)

    assert x.char_pos_tracker.char_pos_map["NITL"].count == 2
    assert x.char_pos_tracker.char_pos_map["NITL"].histogram.get_max_value() == 3
    assert x.char_pos_tracker.char_pos_map["NITL"].histogram.get_min_value() == 3

    assert x.char_pos_tracker.char_pos_map["a"].histogram.get_max_value() == 12
    assert x.char_pos_tracker.char_pos_map["a"].histogram.get_min_value() == 0
    assert y.char_pos_tracker.char_pos_map["NITL"].count == 22
    assert y.char_pos_tracker.char_pos_map["NITL"].histogram.get_max_value() == 39
    new_tracker = x.merge(y)

    assert new_tracker.char_pos_tracker.char_pos_map["a"].histogram.get_max_value() == 40

    assert new_tracker.char_pos_tracker.char_pos_map["NITL"].histogram.get_max_value() == 39
    assert new_tracker.char_pos_tracker.char_pos_map["NITL"].histogram.get_min_value() == 0
    assert new_tracker.char_pos_tracker.char_pos_map["NITL"].count == 24


def test_merge_mod_character_lists():
    x = StringTracker()
    y = StringTracker()
    data = ["abc abc", "93341-1", "912254", "bac tralalala"]
    data_2 = ["geometric inference ", "93341-1", "912254", "bac tralalala", "😀 this is a sale! a ❄️ sale!", "this is a long sentence that ends in an A", None]

    for record in data:
        x.update(record, character_list="ab")
    for record in data_2:
        y.update(record, character_list="a")

    assert x.char_pos_tracker.char_pos_map["NITL"].count == 23
    assert x.char_pos_tracker.char_pos_map["NITL"].histogram.get_max_value() == 11
    assert x.char_pos_tracker.char_pos_map["a"].histogram.get_max_value() == 12
    assert y.char_pos_tracker.char_pos_map["NITL"].count == 102
    assert y.char_pos_tracker.char_pos_map["a"].histogram.get_max_value() == 40
    assert y.char_pos_tracker.char_pos_map["NITL"].histogram.get_max_value() == 39

    x = x.merge(y)

    assert x.char_pos_tracker.char_pos_map["a"].histogram.get_max_value() == 40
    assert x.char_pos_tracker.char_pos_map["NITL"].histogram.get_max_value() == 39
    assert x.char_pos_tracker.char_pos_map["NITL"].count == 125
    assert x.token_length.histogram.get_max_value() == 10


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
    # check case insensitive tracking
    assert x.char_pos_tracker.char_pos_map["o"].count == 5


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
            "stddev": 0.0,
            "isDiscrete": False,
        },
        "charPosTracker": {
            "characterList": "!#$%&()*+,-./0123456789?@[]^_abcdefghijklmnopqrstuvwyz{}",
            "charPosMap": {
                "i": {
                    "count": "1",
                    "min": 1.0,
                    "max": 1.0,
                    "mean": 1.0,
                    "histogram": {"start": 1.0, "end": 1.0000001, "counts": ["1"], "max": 1.0, "min": 1.0, "bins": [1.0, 1.0000001], "n": "1", "width": 0.0},
                    "uniqueCount": {"estimate": 1.0, "upper": 1.0, "lower": 1.0},
                    "quantiles": {
                        "quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
                        "quantileValues": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                    },
                    "stddev": 0.0,
                    "isDiscrete": False,
                },
                "t": {
                    "count": "2",
                    "histogram": {"counts": ["2"], "bins": [0.0, 0.0], "n": "2", "start": 0.0, "end": 0.0, "width": 0.0, "max": 0.0, "min": 0.0},
                    "uniqueCount": {"estimate": 1.0, "upper": 1.0, "lower": 1.0},
                    "quantiles": {
                        "quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
                        "quantileValues": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                    },
                    "min": 0.0,
                    "max": 0.0,
                    "mean": 0.0,
                    "stddev": 0.0,
                    "isDiscrete": False,
                },
                "s": {
                    "count": "1",
                    "histogram": {"counts": ["1"], "bins": [0.0, 0.0], "n": "1", "start": 0.0, "end": 0.0, "width": 0.0, "max": 0.0, "min": 0.0},
                    "uniqueCount": {"estimate": 1.0, "upper": 1.0, "lower": 1.0},
                    "quantiles": {
                        "quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
                        "quantileValues": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                    },
                    "min": 0.0,
                    "max": 0.0,
                    "mean": 0.0,
                    "stddev": 0.0,
                    "isDiscrete": False,
                },
                "n": {
                    "count": "4",
                    "min": 1.0,
                    "max": 1.0,
                    "mean": 1.0,
                    "histogram": {"start": 1.0, "end": 1.0000001, "counts": ["4"], "max": 1.0, "min": 1.0, "bins": [1.0, 1.0000001], "n": "4", "width": 0.0},
                    "uniqueCount": {"estimate": 1.0, "upper": 1.0, "lower": 1.0},
                    "quantiles": {
                        "quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
                        "quantileValues": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                    },
                    "stddev": 0.0,
                    "isDiscrete": False,
                },
                "h": {
                    "count": "1",
                    "min": 1.0,
                    "max": 1.0,
                    "mean": 1.0,
                    "histogram": {"start": 1.0, "end": 1.0000001, "counts": ["1"], "max": 1.0, "min": 1.0, "bins": [1.0, 1.0000001], "n": "1", "width": 0.0},
                    "uniqueCount": {"estimate": 1.0, "upper": 1.0, "lower": 1.0},
                    "quantiles": {
                        "quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
                        "quantileValues": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                    },
                    "stddev": 0.0,
                    "isDiscrete": False,
                },
                "o": {
                    "count": "5",
                    "max": 2.0,
                    "mean": 0.4,
                    "stddev": 0.894427190999916,
                    "histogram": {
                        "end": 2.0000002,
                        "counts": ["4", "1"],
                        "max": 2.0,
                        "bins": [0.0, 1.0000001, 2.0000002],
                        "n": "5",
                        "start": 0.0,
                        "width": 0.0,
                        "min": 0.0,
                    },
                    "uniqueCount": {"estimate": 2.0, "upper": 2.0, "lower": 2.0},
                    "quantiles": {
                        "quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
                        "quantileValues": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.0, 2.0, 2.0],
                    },
                    "min": 0.0,
                    "isDiscrete": False,
                },
                "NITL": {
                    "count": "1",
                    "min": 2.0,
                    "max": 2.0,
                    "mean": 2.0,
                    "histogram": {"start": 2.0, "end": 2.0000002, "counts": ["1"], "max": 2.0, "min": 2.0, "bins": [2.0, 2.0000002], "n": "1", "width": 0.0},
                    "uniqueCount": {"estimate": 1.0, "upper": 1.0, "lower": 1.0},
                    "quantiles": {
                        "quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
                        "quantileValues": [2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0],
                    },
                    "stddev": 0.0,
                    "isDiscrete": False,
                },
                "w": {
                    "count": "1",
                    "min": 1.0,
                    "max": 1.0,
                    "mean": 1.0,
                    "histogram": {"start": 1.0, "end": 1.0000001, "counts": ["1"], "max": 1.0, "min": 1.0, "bins": [1.0, 1.0000001], "n": "1", "width": 0.0},
                    "uniqueCount": {"estimate": 1.0, "upper": 1.0, "lower": 1.0},
                    "quantiles": {
                        "quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
                        "quantileValues": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                    },
                    "stddev": 0.0,
                    "isDiscrete": False,
                },
                "e": {
                    "count": "6",
                    "min": 2.0,
                    "max": 4.0,
                    "mean": 2.5,
                    "stddev": 0.8366600265340756,
                    "histogram": {
                        "start": 2.0,
                        "end": 4.0000004,
                        "counts": ["5", "1"],
                        "max": 4.0,
                        "min": 2.0,
                        "bins": [2.0, 3.0000002, 4.0000004],
                        "n": "6",
                        "width": 0.0,
                    },
                    "uniqueCount": {"estimate": 3.0, "upper": 3.0, "lower": 3.0},
                    "quantiles": {
                        "quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
                        "quantileValues": [2.0, 2.0, 2.0, 2.0, 2.0, 3.0, 4.0, 4.0, 4.0],
                    },
                    "isDiscrete": False,
                },
                "r": {
                    "count": "1",
                    "min": 2.0,
                    "max": 2.0,
                    "mean": 2.0,
                    "histogram": {"start": 2.0, "end": 2.0000002, "counts": ["1"], "max": 2.0, "min": 2.0, "bins": [2.0, 2.0000002], "n": "1", "width": 0.0},
                    "uniqueCount": {"estimate": 1.0, "upper": 1.0, "lower": 1.0},
                    "quantiles": {
                        "quantiles": [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
                        "quantileValues": [2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0],
                    },
                    "stddev": 0.0,
                    "isDiscrete": False,
                },
            },
        },
    }
    expected_items = pd.DataFrame(expected["frequent"]["items"]).sort_values(["value", "estimate"])
    expected["frequent"].pop("items")

    actual = message_to_dict(x.to_summary())
    actual_items = pd.DataFrame(actual["frequent"]["items"]).sort_values(["value", "estimate"])
    actual["frequent"].pop("items")

    assert expected == actual
    pd.testing.assert_frame_equal(actual_items.reset_index(drop=True).sort_index(axis=1), expected_items.reset_index(drop=True).sort_index(axis=1))


def test_string_tracker_merge():
    x = StringTracker()
    data = ["one", "two", "three", "one", "one", "One test", "six", None, None]
    for record in data:
        x.update(record)
    assert x.token_length.histogram.get_max_value() == 2
    assert x.token_length.histogram.get_min_value() == 1
    x2 = StringTracker()
    data = ["this is a long sentence that ends in an A"]
    for record in data:
        x2.update(record)
    assert x2.token_length.histogram.get_max_value() == 10
    assert x2.token_length.histogram.get_min_value() == 10
    new_string_track = x2.merge(x)
    assert new_string_track.token_length.histogram.get_max_value() == 10
    assert new_string_track.token_length.histogram.get_min_value() == 1
