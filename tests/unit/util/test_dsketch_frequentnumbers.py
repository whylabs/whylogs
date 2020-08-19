"""
"""
import datasketches
import pytest
from testutil import compare_frequent_items

from whylabs.logs.util import dsketch

NUMBER_SKETCH_VALS = [1, 1, 1, 2, 3, 4, 4, 5.0]
STRING_SKETCH_VALS = ["1", "1", "1", "2", "3", "4", "4", "5.0"]


@pytest.fixture(scope="module")
def string_sketch():
    items = datasketches.frequent_strings_sketch(32)
    for val in STRING_SKETCH_VALS:
        assert isinstance(val, str)
        items.update(val)
    return items


@pytest.fixture(scope="module")
def number_sketch():
    items = dsketch.FrequentNumbersSketch(32)
    for val in NUMBER_SKETCH_VALS:
        assert isinstance(val, (float, int))
        items.update(val)
    return items


def test_summary_returns_correct_estimates(number_sketch):
    summary = number_sketch.to_summary()
    estimates = []
    for record_list in (summary.longs, summary.doubles):
        for xi in record_list:
            estimates.append((xi.value, xi.estimate))
    estimates_set = set(estimates)
    expected = {
        (1, 3),
        (2, 1),
        (3, 1),
        (4, 2),
        (5.0, 1),
    }
    assert expected == estimates_set
    assert len(estimates) == len(expected)


def test_number_and_string_apriori_error_equal(string_sketch, number_sketch):
    # Try it out for a few combos of map sizes
    map_sizes = [1, 2, 32, 120]
    weights = [0, 10, 50, 100]
    for map_size, w in zip(map_sizes, weights):
        assert string_sketch.get_apriori_error(
            map_size, w
        ) == number_sketch.get_apriori_error(map_size, w)


def test_number_and_string_epsilon_equal(string_sketch, number_sketch):
    for lg in [1, 3, 32, 120]:
        assert string_sketch.get_epsilon_for_lg_size(
            lg
        ) == number_sketch.get_epsilon_for_lg_size(lg)


def test_number_and_string_estimates_equal(string_sketch, number_sketch):
    str_vals = STRING_SKETCH_VALS + ["0"]
    num_vals = NUMBER_SKETCH_VALS + [0]
    for s, n in zip(str_vals, num_vals):
        assert string_sketch.get_estimate(s) == number_sketch.get_estimate(n)
        assert string_sketch.get_lower_bound(s) == number_sketch.get_lower_bound(n)
        assert string_sketch.get_upper_bound(s) == number_sketch.get_upper_bound(n)


def test_frequent_items_correct(number_sketch):
    items = number_sketch.get_frequent_items()
    true = [
        (1, 3, 3, 3),
        (4, 2, 2, 2),
        (2, 1, 1, 1),
        (3, 1, 1, 1),
        (5.0, 1, 1, 1),
    ]
    compare_frequent_items(true, items)


def test_num_active_items_equal(string_sketch, number_sketch):
    assert string_sketch.get_num_active_items() == number_sketch.get_num_active_items()


def test_sketch_epislon_equal(string_sketch, number_sketch):
    assert string_sketch.get_sketch_epsilon() == number_sketch.get_sketch_epsilon()


def test_total_weight_equal(string_sketch, number_sketch):
    assert string_sketch.get_total_weight() == number_sketch.get_total_weight()


def test_empty_sketch_returns_empty():
    assert dsketch.FrequentNumbersSketch(32).is_empty()


def test_non_empty_sketch_returns_not_empty(number_sketch):
    assert number_sketch.is_empty() is False


def test_sketch_roundtrip_serialize(number_sketch):
    msg = number_sketch.serialize()
    assert len(msg) == number_sketch.get_serialized_size_bytes()
    round_trip = dsketch.FrequentNumbersSketch.deserialize(msg)
    assert round_trip.get_frequent_items() == number_sketch.get_frequent_items()


def test_to_string_returns_string(number_sketch):
    assert isinstance(number_sketch.to_string(), str)


def test_protobuf_roundtrip(number_sketch):
    msg = number_sketch.to_protobuf()
    sketch2 = dsketch.FrequentNumbersSketch.from_protobuf(msg)
    assert number_sketch.get_frequent_items() == sketch2.get_frequent_items()
