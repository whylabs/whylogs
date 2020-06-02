"""
"""
import datasketches

from whylabs.logs.core import summaryconverters
from whylabs.logs.proto import HistogramSummary


def _hist_summary_check(summary, vals: list):
    assert isinstance(summary, HistogramSummary)
    assert summary.n == len(vals)
    assert summary.min == min(vals)
    assert summary.max == max(vals)
    assert len(summary.counts) == (len(summary.bins) - 1)
    assert summary.bins[0] == summary.start
    assert summary.bins[-1] == summary.end


def test_histogram_summary():
    hist = datasketches.kll_floats_sketch(256)
    vals = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    vals = [float(v) for v in vals]
    for val in vals:
        hist.update(val)

    summary = summaryconverters.from_kll_floats_sketch(hist)
    _hist_summary_check(summary, vals)
    assert len(summary.counts) > 1


def test_single_value_histogram_summary():
    hist = datasketches.kll_floats_sketch(256)
    vals = 30 * [1]
    vals = [float(v) for v in vals]
    for val in vals:
        hist.update(val)

    summary = summaryconverters.from_kll_floats_sketch(hist)
    _hist_summary_check(summary, vals)
    assert len(summary.counts) == 1
