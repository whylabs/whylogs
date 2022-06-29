import pytest

from whylogs.viz.extensions.reports.summary_drift import SummaryDriftReport


class TestReports(object):
    @pytest.fixture
    def summary_drift_report(self, profile_view):
        return SummaryDriftReport(ref_view=profile_view, target_view=profile_view)

    def test_summary_drift_report_returns_html(self, summary_drift_report):
        html_report = summary_drift_report.report()
        assert isinstance(html_report, str)
        assert "<div>" in html_report

    def test_exception_if_not_both_profiles(self, profile_view):
        with pytest.raises(ValueError, match="This method has to get both target and reference profiles"):
            report = SummaryDriftReport(ref_view=profile_view, target_view=None)
            report.report()
