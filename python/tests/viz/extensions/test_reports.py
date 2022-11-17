import pytest
from IPython.core.display import HTML

from whylogs.viz.extensions.reports.profile_summary import ProfileSummaryReport
from whylogs.viz.extensions.reports.summary_drift import SummaryDriftReport


class TestReports(object):
    @pytest.fixture
    def summary_drift_report(self, profile_view):
        return SummaryDriftReport(ref_view=profile_view, target_view=profile_view)

    @pytest.fixture
    def profile_summary_report(self, profile_view):
        return ProfileSummaryReport(target_view=profile_view)

    def test_summary_drift_report_returns_html(self, summary_drift_report):
        html_report = summary_drift_report.report()
        assert isinstance(html_report, HTML)
        assert "<div>" in html_report.data

    def test_exception_if_not_both_profiles(self, profile_view):
        with pytest.raises(ValueError, match="This method has to get both target and reference profiles"):
            report = SummaryDriftReport(ref_view=profile_view, target_view=None)
            report.report()

    def test_summary_profile_returns_html(self, profile_summary_report):
        html_report = profile_summary_report.report()
        assert isinstance(html_report, HTML)
        assert "<div>" in html_report.data

    def test_exception_if_not_profile(self, profile_view):
        with pytest.raises(ValueError, match="This method has to get target Dataset Profile View"):
            report = ProfileSummaryReport(target_view=None)
            report.report()
