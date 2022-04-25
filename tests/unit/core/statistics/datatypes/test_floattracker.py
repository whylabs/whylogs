import numpy as np

from whylogs.core.statistics.datatypes import FloatTracker


def test_values_are_min_max():
    first = FloatTracker()
    vals1 = [1.0, 2.0, 3.0]
    for val in vals1:
        first.update(val)

    assert first.count == len(vals1)
    assert first.max == max(vals1)
    assert first.min == min(vals1)
    assert first.sum == sum(vals1)


def test_np_float():
    first = FloatTracker()
    float32_array = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    for val in float32_array:
        first.update(val)

    assert first.count == len(float32_array)
    assert first.max == max(float32_array)
    assert first.min == min(float32_array)
    assert first.sum == sum(float32_array)

    second = FloatTracker()
    float64_array = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    for val in float64_array:
        second.update(val)

    assert second.count == len(float64_array)
    assert second.max == max(float64_array)
    assert second.min == min(float64_array)
    assert second.sum == sum(float64_array)


def test_merge_floattrackers_should_addup():
    first = FloatTracker()
    vals1 = [1.0, 2.0, 3.0]
    for val in vals1:
        first.update(val)

    assert first.count == len(vals1)
    assert first.max == max(vals1)
    assert first.min == min(vals1)
    assert first.sum == sum(vals1)

    second = FloatTracker()
    vals2 = [4.0, 5.0, 6.0]
    for val in vals2:
        second.update(val)

    assert second.count == len(vals2)
    assert second.max == max(vals2)
    assert second.min == min(vals2)
    assert second.sum == sum(vals2)

    all_vals = vals1 + vals2
    merge_first = first.merge(second)
    assert merge_first.count == len(all_vals)
    assert merge_first.max == max(all_vals)
    assert merge_first.min == min(all_vals)
    assert merge_first.sum == sum(all_vals)

    merge_second = second.merge(first)
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
    merge_first_tracker_32_and_simple = float32_tracker.merge(simple_tracker)
    assert merge_first_tracker_32_and_simple.count == len(merge_32_and_simple)
    assert merge_first_tracker_32_and_simple.max == max(merge_32_and_simple)
    assert merge_first_tracker_32_and_simple.min == min(merge_32_and_simple)
    assert merge_first_tracker_32_and_simple.sum == sum(merge_32_and_simple)

    merge_second_tracker_32_and_simple = simple_tracker.merge(float32_tracker)
    assert merge_first_tracker_32_and_simple.__dict__ == merge_second_tracker_32_and_simple.__dict__

    merge_64_and_simple = float64_array.tolist() + vals2
    merge_first_tracker_64_and_simple = float64_tracker.merge(simple_tracker)
    assert merge_first_tracker_64_and_simple.count == len(merge_64_and_simple)
    assert merge_first_tracker_64_and_simple.max == max(merge_64_and_simple)
    assert merge_first_tracker_64_and_simple.min == min(merge_64_and_simple)
    assert merge_first_tracker_64_and_simple.sum == sum(merge_64_and_simple)

    merge_second_tracker_64_and_simple = simple_tracker.merge(float64_tracker)
    assert merge_first_tracker_64_and_simple.__dict__ == merge_second_tracker_64_and_simple.__dict__

    merge_64_and_32 = np.concatenate((float64_array, float32_array))
    merge_first_tracker_64_and_32 = float64_tracker.merge(float32_tracker)
    assert merge_first_tracker_64_and_32.count == len(merge_64_and_32)
    assert merge_first_tracker_64_and_32.max == max(merge_64_and_32)
    assert merge_first_tracker_64_and_32.min == min(merge_64_and_32)
    assert merge_first_tracker_64_and_32.sum == sum(merge_64_and_32)

    merge_second_tracker_64_and_32 = float32_tracker.merge(float64_tracker)
    assert merge_first_tracker_64_and_32.__dict__ == merge_second_tracker_64_and_32.__dict__
