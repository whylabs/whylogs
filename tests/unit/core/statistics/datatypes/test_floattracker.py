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
