"""
"""
from whylabs.logs.util import dsketch
import datasketches
from testutil import compare_frequent_items
import json
import numpy as np
decode_item = dsketch.FrequentItemsSketch._decode_item

NUMBERS = [1, 1, 1, 2, 3, 4, 4, 5.0, 5.0, 4.0, 1e90]
STRINGS = ['a', 'b', 'hello world', 'hello World', 'a', 'a', 'b']
BOOLS = [True, False, True]

ALL_VALS = NUMBERS + STRINGS + BOOLS
LG_K = 32


def make_sketch_and_track(vals=[], lg_k=LG_K):
    sketch = dsketch.FrequentItemsSketch(lg_k)
    for v in vals:
        sketch.update(v)
    return sketch


def make_datasketch_and_track(vals=[], lg_k=LG_K):
    sketch = datasketches.frequent_strings_sketch(lg_k)
    for v in vals:
        sketch.update(v)
    return sketch


def track_and_validate_summary_estimates(vals: list, expected: set):
    assert isinstance(expected, set)
    sketch = make_sketch_and_track(vals)
    summary = sketch.to_summary()
    estimates = [(decode_item(xi.json_value), xi.estimate)
                 for xi in summary.items]
    assert expected == set(estimates)
    assert len(estimates) == len(expected)


def test_number_summary_returns_correct_estimates():
    expected = {
        (1, 3),
        (2, 1),
        (3, 1),
        (4, 2),
        (5.0, 2),
        (4.0, 1),
        (1.0e90, 1),
    }
    track_and_validate_summary_estimates(NUMBERS, expected)


def test_bool_summary_returns_correct_estimates():
    expected = {
        (True, 2),
        (False, 1),
    }
    track_and_validate_summary_estimates(BOOLS, expected)


def test_string_summary_returns_correct_estimates():
    expected = {
        ('a', 3),
        ('b', 2),
        ('hello world', 1),
        ('hello World', 1),
    }
    track_and_validate_summary_estimates(STRINGS, expected)


def test_mixed_summary_returns_correct_estimates():
    expected = {
        (1, 3),
        (2, 1),
        (3, 1),
        (4, 2),
        (5.0, 2),
        (4.0, 1),
        (1.0e90, 1),
        (True, 2),
        (False, 1),
        ('a', 3),
        ('b', 2),
        ('hello world', 1),
        ('hello World', 1),
    }
    track_and_validate_summary_estimates(ALL_VALS, expected)


def test_bool_summary_returns_correct_estimates():
    sketch = make_sketch_and_track(BOOLS)
    summary = sketch.to_summary()
    estimates = [(json.loads(xi.json_value), xi.estimate)
                 for xi in summary.items]
    expected = {
        (True, 2),
        (False, 1),
    }
    assert expected == set(estimates)
    assert len(estimates) == len(expected)


def test_apriori_error_equal_datasketches_error():
    # Try it out for a few combos of map sizes
    map_sizes = [1, 2, 32, 120]
    weights = [0, 10, 50, 100]
    sketch = dsketch.FrequentItemsSketch(LG_K)
    strings_sketch = datasketches.frequent_strings_sketch(LG_K)
    for map_size, w in zip(map_sizes, weights):
        assert sketch.get_apriori_error(map_size, w) \
            == strings_sketch.get_apriori_error(map_size, w)


def test_string_epsilon_equals_datasketches():
    sketch = dsketch.FrequentItemsSketch(LG_K)
    strings_sketch = datasketches.frequent_strings_sketch(LG_K)
    for lg in [1, 3, 32, 120]:
        assert sketch.get_epsilon_for_lg_size(lg) \
            == strings_sketch.get_epsilon_for_lg_size(lg)


def test_estimates_equal_datasketches():
    sketch = make_sketch_and_track(STRINGS)
    strings_sketch = make_datasketch_and_track(STRINGS)
    for v in STRINGS:
        assert sketch.get_estimate(v) == strings_sketch.get_estimate(v)
        assert sketch.get_lower_bound(v) == strings_sketch.get_lower_bound(v)
        assert sketch.get_upper_bound(v) == strings_sketch.get_upper_bound(v)


def test_frequent_items_correct():
    sketch = make_sketch_and_track(ALL_VALS)
    items = sketch.get_frequent_items()
    true_items = [
        (1, 3, 3, 3),
        (2, 1, 1, 1),
        (3, 1, 1, 1),
        (4, 2, 2, 2),
        (5.0, 2, 2, 2),
        (4.0, 1, 1, 1),
        (1.0e90, 1, 1, 1),
        (True, 2, 2, 2),
        (False, 1, 1, 1),
        ('a', 3, 3, 3),
        ('b', 2, 2, 2),
        ('hello world', 1, 1, 1),
        ('hello World', 1, 1, 1),
    ]
    compare_frequent_items(true_items, items)


def test_correct_num_active_items():
    sketch = make_sketch_and_track(ALL_VALS)
    assert sketch.get_num_active_items() == 13
    sketch = dsketch.FrequentItemsSketch()
    assert sketch.get_num_active_items() == 0
    sketch.update(1)
    assert sketch.get_num_active_items() == 1


def test_sketch_epsilon_equals_datasketches():
    sketch = make_sketch_and_track(STRINGS)
    strings_sketch = make_datasketch_and_track(STRINGS)
    assert sketch.get_sketch_epsilon() == strings_sketch.get_sketch_epsilon()

def test_sketch_epsilon_correct():
    sketch = make_sketch_and_track(STRINGS)
    assert sketch.get_sketch_epsilon() == 3.5


def test_total_weight_correct():
    # Default weights
    sketch = make_sketch_and_track(STRINGS)
    assert sketch.get_total_weight() == len(STRINGS)

    # Integer weights
    sketch = dsketch.FrequentItemsSketch()
    weights = list(range(len(ALL_VALS)))
    for v, w in zip(ALL_VALS, weights):
        sketch.update(v, w)
    assert sketch.get_total_weight() == sum(weights)


def test_empty_sketch_returns_empty():
    assert dsketch.FrequentItemsSketch(32).is_empty()


def test_non_empty_sketch_returns_not_empty():
    sketch = make_sketch_and_track(ALL_VALS)
    assert sketch.is_empty() is False


def test_sketch_roundtrip_serialize():
    sketch = make_sketch_and_track(ALL_VALS)
    msg = sketch.serialize()
    assert len(msg) == sketch.get_serialized_size_bytes()
    round_trip = dsketch.FrequentItemsSketch.deserialize(msg)
    compare_frequent_items(round_trip.get_frequent_items(),
                           sketch.get_frequent_items())


def test_to_string_returns_string():
    sketch = make_sketch_and_track(ALL_VALS)
    assert isinstance(sketch.to_string(), str)


def test_protobuf_roundtrip():
    sketch = make_sketch_and_track(ALL_VALS)
    msg = sketch.to_protobuf()
    sketch2 = dsketch.FrequentItemsSketch.from_protobuf(msg)
    compare_frequent_items(sketch.get_frequent_items(),
                           sketch2.get_frequent_items())


def test_copy_empty_returns_empty():
    sketch = dsketch.FrequentItemsSketch()
    assert sketch.copy().is_empty()


def test_copy_returns_correct_type():
    # Empty sketch
    sketch = dsketch.FrequentItemsSketch()
    copy = sketch.copy()
    assert type(copy) == type(sketch)

    # Sketch with an item
    sketch.update(1)
    copy = sketch.copy()
    assert type(copy) == type(sketch)


def test_copy_gives_same_results():
    sketch = make_sketch_and_track(ALL_VALS)
    copy = sketch.copy()
    compare_frequent_items(sketch.get_frequent_items(),
                           copy.get_frequent_items())


def test_merge_is_not_in_place():
    sketch = make_sketch_and_track(ALL_VALS)
    items = sketch.get_frequent_items()
    sketch2 = make_sketch_and_track(NUMBERS)
    merged = sketch.merge(sketch2)
    assert items == sketch.get_frequent_items()
    assert items != merged.get_frequent_items()


def test_merge_empty_gives_same_result():
    # Make sketches
    sketch = make_sketch_and_track(ALL_VALS)
    empty = dsketch.FrequentItemsSketch()
    items = sketch.get_frequent_items()
    # Merge empty into full
    merged = sketch.merge(empty)
    compare_frequent_items(items, merged.get_frequent_items())
    # Merge full into empty
    merged = empty.merge(sketch)
    compare_frequent_items(items, merged.get_frequent_items())


def test_merge_gives_correct_values():
    sketch = make_sketch_and_track(ALL_VALS)
    merged = sketch.merge(sketch.copy())
    items = sketch.get_frequent_items()
    merged_items = merged.get_frequent_items()
    # All counts should just be doubled!
    expected = []
    for item in items:
        new_item = (item[0], 2*item[1], 2*item[2], 2*item[3])
        expected.append(new_item)
    compare_frequent_items(expected, merged_items)


def test_empty_sketch_summary_returns_none():
    sketch = dsketch.FrequentItemsSketch()
    summary = sketch.to_summary()
    assert summary is None


def test_null_lg_max_k_protobuf_value():
    sketch = dsketch.FrequentItemsSketch()
    sketch.lg_max_k = None
    msg = sketch.to_protobuf()
    assert msg.lg_max_k == -1

    sketch2 = dsketch.FrequentItemsSketch.from_protobuf(msg)
    assert sketch2.lg_max_k is None


def test_numpy_datatypes_are_tracked():
    numpy_ints = np.arange(3)
    numpy_floats = np.arange(3, dtype=float)

    int_sketch = make_sketch_and_track(numpy_ints)
    for v in numpy_ints:
        assert int_sketch.get_estimate(v) == 1
    for v in numpy_floats:
        assert int_sketch.get_estimate(v) == 0

    float_sketch = make_sketch_and_track(numpy_floats)
    for v in numpy_floats:
        assert float_sketch.get_estimate(v) == 1
    for v in numpy_ints:
        assert float_sketch.get_estimate(v) == 0
