import html
import logging
from abc import ABC
from typing import Optional

from IPython.core.display import HTML

from whylogs import DatasetProfileView
from whylogs.viz.enums.enums import PageSpec, PageSpecEnum
from whylogs.viz.utils.html_template_utils import get_compiled_template
from whylogs.viz.utils.profile_viz_calculations import generate_summaries

logger = logging.getLogger(__name__)


class HTMLReport(ABC):
    def __init__(
        self,
        ref_view: Optional[DatasetProfileView] = None,
        target_view: Optional[DatasetProfileView] = None,
        height: Optional[str] = None,
    ):
        self.ref_view = ref_view
        self.target_view = target_view
        self.height = height or None

    def display(self, template: str, page_spec: PageSpec) -> HTML:
        if not self.height:
            self.height = page_spec.height
        iframe = f"""<div></div><iframe srcdoc="{html.escape(template)}" width=100% height={self.height}
        frameBorder=0></iframe>"""
        display = HTML(iframe)
        return display


class SummaryDriftReport(HTMLReport):
    def __init__(self, ref_view: DatasetProfileView, target_view: DatasetProfileView, height: Optional[str] = None):
        super().__init__(ref_view=ref_view, target_view=target_view, height=height)

    def report(self):
        page_spec = PageSpecEnum.SUMMARY_REPORT.value
        template = get_compiled_template(page_spec.html)

        profiles_summary = generate_summaries(self.target_view, self.ref_view, config=None)
        rendered_template = template(profiles_summary)
        summary_drift_report = self.display(rendered_template, page_spec)
        return summary_drift_report


class DistributionChart(HTMLReport):
    pass


class DifferenceDistributionChart(HTMLReport):
    pass


class ConstraintsReport(HTMLReport):
    pass


class FeatureStatisticsReport(HTMLReport):
    pass


class DoubleHistogram(HTMLReport):
    pass
