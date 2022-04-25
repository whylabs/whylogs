import numpy as np

from whylogs.core.statistics.datatypes import FloatTracker


def _test_tracker_vs_array(tracker_of_array, array):
    for val in array:
        tracker_of_array.update(val)
    assert tracker_of_array.count == len(array)
    assert tracker_of_array.max == max(array)
    assert tracker_of_array.min == min(array)
    assert tracker_of_array.sum == sum(array)


def test_values_are_min_max():
    first = FloatTracker()
    vals1 = [1.0, 2.0, 3.0]
    _test_tracker_vs_array(first, vals1)


def test_np_float():
    first = FloatTracker()
    float32_array = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    _test_tracker_vs_array(first, float32_array)

    second = FloatTracker()
    float64_array = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    _test_tracker_vs_array(second, float64_array)


def test_merge_floattrackers_should_addup():
    first = FloatTracker()
    vals1 = [1.0, 2.0, 3.0]
    _test_tracker_vs_array(first, vals1)

    second = FloatTracker()
    vals2 = [4.0, 5.0, 6.0]
    _test_tracker_vs_array(second, vals2)

    all_vals = vals1 + vals2
    _test_merged_tracker_vs_arrays(all_vals, first, second)


def _test_merged_tracker_vs_arrays(combined_arrays, first_tracker, second_tracker):
    merge_first = first_tracker.merge(second_tracker)
    assert merge_first.count == len(combined_arrays)
    assert merge_first.max == max(combined_arrays)
    assert merge_first.min == min(combined_arrays)
    assert merge_first.sum == sum(combined_arrays)
    merge_second = second_tracker.merge(first_tracker)
    assert merge_second.__dict__ == merge_first.__dict__


def test_merge_floattrackers_should_addup():
    float32_tracker = FloatTracker()
    float32_array = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    for val in float32_array:
        float32_tracker.update(val)

    float64_tracker = FloatTracker()
    float64_array = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    for val in float64_array:
        float64_tracker.update(val)

    simple_tracker = FloatTracker()
    vals2 = [4.0, 5.0, 6.0]
    for val in vals2:
        simple_tracker.update(val)

    merge_32_and_simple = float32_array.tolist() + vals2
    _test_merged_tracker_vs_arrays(merge_32_and_simple, float32_tracker, simple_tracker)

    merge_64_and_simple = float64_array.tolist() + vals2
    _test_merged_tracker_vs_arrays(merge_64_and_simple, float64_tracker, simple_tracker)

    merge_64_and_32 = np.concatenate((float64_array, float32_array))
    _test_merged_tracker_vs_arrays(merge_64_and_32, float32_tracker, float64_tracker)
