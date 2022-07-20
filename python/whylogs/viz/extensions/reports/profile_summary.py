import logging
from typing import Optional

from whylogs import DatasetProfileView
from whylogs.viz.enums.enums import PageSpecEnum
from whylogs.viz.extensions.reports.html_report import HTMLReport
from whylogs.viz.utils.html_template_utils import get_compiled_template
from whylogs.viz.utils.profile_viz_calculations import generate_profile_summary

logger = logging.getLogger(__name__)


class ProfileSummaryReport(HTMLReport):
    def __init__(self, target_view: DatasetProfileView, height: Optional[str] = None):
        super().__init__(target_view=target_view, height=height)

    def report(self) -> str:
        page_spec = PageSpecEnum.PROFILE_SUMMARY.value
        template = get_compiled_template(page_spec.html)

        profile_summary = generate_profile_summary(self.target_view, config=None)
        rendered_template = template(profile_summary)
        profile_summary_report = self.display(rendered_template, page_spec)
        return profile_summary_report
