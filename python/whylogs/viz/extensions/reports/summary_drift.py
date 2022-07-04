import logging
from typing import Optional

from whylogs import DatasetProfileView
from whylogs.viz.enums.enums import PageSpecEnum
from whylogs.viz.extensions.reports.html_report import HTMLReport
from whylogs.viz.utils.html_template_utils import get_compiled_template
from whylogs.viz.utils.profile_viz_calculations import generate_summaries

logger = logging.getLogger(__name__)


class SummaryDriftReport(HTMLReport):
    def __init__(self, ref_view: DatasetProfileView, target_view: DatasetProfileView, height: Optional[str] = None):
        super().__init__(ref_view=ref_view, target_view=target_view, height=height)

    def report(self) -> str:
        page_spec = PageSpecEnum.SUMMARY_REPORT.value
        template = get_compiled_template(page_spec.html)

        profiles_summary = generate_summaries(self.target_view, self.ref_view, config=None)
        rendered_template = template(profiles_summary)
        summary_drift_report = self.display(rendered_template, page_spec)
        return summary_drift_report
