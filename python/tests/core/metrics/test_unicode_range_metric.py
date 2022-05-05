import numpy as np
import pytest

from whylogs.core.metrics.unicode_range import UnicodeRangeMetric
from whylogs.core.preprocessing import PreprocessedColumn


def test_unicode_range_metric() -> None:
    metric = UnicodeRangeMetric({"digits": (48, 57), "alpha": (97, 122)})
    col = PreprocessedColumn.apply(["1", "12", "123", "1234a", "abc", "abc123"])
    digit_counts = [1, 2, 3, 4, 0, 3]
    alpha_counts = [0, 0, 0, 1, 3, 3]
    metric.columnar_update(col)

    assert metric.submetrics["digits"].mean.value == np.array(digit_counts).mean()
    assert metric.submetrics["alpha"].mean.value == np.array(alpha_counts).mean()


@pytest.mark.parametrize(
    "bad_range",
    [
        {"bad": (-4, -1)},
        {"bad": (4, 1)},
        {"bad": (1, 0x11FFFF)},
        {"very:bad": (1, 2)},
        {"also/bad": (1, 2)},
    ],
)
def test_unicode_range_metric_invalid_initialization(bad_range):
    with pytest.raises(ValueError):
        UnicodeRangeMetric(bad_range)


def test_unicode_range_metric_serialization() -> None:
    metric = UnicodeRangeMetric({"digits": (48, 57), "alpha": (97, 122)})
    col = PreprocessedColumn.apply(["1", "12", "123", "1234a", "abc", "abc123"])
    metric.columnar_update(col)
    msg = metric.to_protobuf()
    deserialized = UnicodeRangeMetric.from_protobuf(msg)

    assert deserialized.submetrics["digits"].mean.value == metric.submetrics["digits"].mean.value
    assert deserialized.submetrics["alpha"].mean.value == metric.submetrics["alpha"].mean.value


def test_unicode_range_metric_summary() -> None:
    metric = UnicodeRangeMetric({"digits": (48, 57), "alpha": (97, 122)})
    col = PreprocessedColumn.apply(["1", "12", "123", "1234a", "abc", "abc123"])
    metric.columnar_update(col)
    summary = metric.to_summary_dict(None)

    assert "unicode_range/digits/mean" in summary
    assert "unicode_range/alpha/mean" in summary


def test_unicode_range_metric_merge() -> None:
    metric1 = UnicodeRangeMetric({"digits": (48, 57), "alpha": (97, 122)})
    metric2 = UnicodeRangeMetric({"digits": (48, 57), "alpha": (97, 122)})
    col1 = PreprocessedColumn.apply(["1", "12", "123"])
    col2 = PreprocessedColumn.apply(["1234a", "abc", "abc123"])
    metric1.columnar_update(col1)
    metric2.columnar_update(col2)
    merged = metric1 + metric2

    assert merged.submetrics["digits"].kll.value.get_n() == 6
    assert merged.submetrics["digits"].kll.value.get_min_value() == 0
    assert merged.submetrics["digits"].kll.value.get_max_value() == 4

    assert merged.submetrics["alpha"].kll.value.get_n() == 6
    assert merged.submetrics["alpha"].kll.value.get_min_value() == 0
    assert merged.submetrics["alpha"].kll.value.get_max_value() == 3
