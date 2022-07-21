from dataclasses import dataclass
from enum import Enum


@dataclass
class PageSpec:
    html: str
    height: str


class PageSpecEnum(Enum):
    PROFILE_SUMMARY = PageSpec(html="index-hbs-cdn-all-in-jupyter-profile-summary.html", height="1000px")
    SUMMARY_REPORT = PageSpec(html="index-hbs-cdn-all-in-for-jupyter-notebook.html", height="1000px")
    DOUBLE_HISTOGRAM = PageSpec(html="index-hbs-cdn-all-in-jupyter-distribution-chart.html", height="300px")
    DISTRIBUTION_CHART = PageSpec(html="index-hbs-cdn-all-in-jupyter-bar-chart.html", height="277px")
    DIFFERENCED_CHART = PageSpec(html="index-hbs-cdn-all-in-jupyter-differenced-chart.html", height="277px")
    FEATURE_STATISTICS = PageSpec(html="index-hbs-cdn-all-in-jupyter-feature-summary-statistics.html", height="650px")
    CONSTRAINTS_REPORT = PageSpec(html="index-hbs-cdn-all-in-jupyter-constraints-report.html", height="750px")
