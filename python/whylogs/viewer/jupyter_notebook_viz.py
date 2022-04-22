import html
import json
import logging
import os
from typing import Optional

from IPython.core.display import HTML  # type: ignore

from whylogs.core import DatasetProfile

from .utils.profile_viz_calculations import add_feature_statistics

try:
    from pybars import Compiler  # type: ignore
except:  # noqa
    pass

logger = logging.getLogger(__name__)
_MY_DIR = os.path.realpath(os.path.dirname(__file__))


def _get_template_path(html_file_name: str) -> str:
    template_path = os.path.abspath(os.path.join(_MY_DIR, "html", "templates", html_file_name))
    return template_path


def _get_compiled_template(template_name: str) -> "Compiler":
    template_path = _get_template_path(template_name)
    try:
        from pybars import Compiler
    except ImportError as e:
        Compiler = None
        logger.debug(str(e))
        logger.warning("Unable to load pybars; install pybars3 to load profile directly from the current session ")

    with open(template_path, "r") as file_with_template:
        source = file_with_template.read()
    compiler = Compiler()
    template = compiler.compile(source)
    return template


class NotebookProfileVisualizer:
    SUMMARY_REPORT_TEMPLATE_NAME = "index-hbs-cdn-all-in-for-jupyter-notebook.html"
    DOUBLE_HISTOGRAM_TEMPLATE_NAME = "index-hbs-cdn-all-in-jupyter-distribution-chart.html"
    DISTRIBUTION_CHART_TEMPLATE_NAME = "index-hbs-cdn-all-in-jupyter-bar-chart.html"
    DIFFERENCED_CHART_TEMPLATE_NAME = "index-hbs-cdn-all-in-jupyter-differenced-chart.html"
    FEATURE_STATISTICS_TEMPLATE_NAME = "index-hbs-cdn-all-in-jupyter-feature-summary-statistics.html"
    CONSTRAINTS_REPORT_TEMPLATE_NAME = "index-hbs-cdn-all-in-jupyter-constraints-report.html"
    PAGE_SIZES = {
        SUMMARY_REPORT_TEMPLATE_NAME: "1000px",
        DOUBLE_HISTOGRAM_TEMPLATE_NAME: "300px",
        DISTRIBUTION_CHART_TEMPLATE_NAME: "277px",
        DIFFERENCED_CHART_TEMPLATE_NAME: "277px",
        FEATURE_STATISTICS_TEMPLATE_NAME: "650px",
        CONSTRAINTS_REPORT_TEMPLATE_NAME: "750PX",
    }

    def _display_rendered_template(self, template: str, template_name: str, height: Optional[str]) -> HTML:
        if not height:
            height = self.PAGE_SIZES[template_name]
        iframe = f"""<div></div><iframe srcdoc="{html.escape(template)}" width=100% height={height}
        frameBorder=0></iframe>"""
        return HTML(iframe)

    def set_profiles(self, target_profile: DatasetProfile = None, reference_profile: DatasetProfile = None) -> None:
        self._target_profile = target_profile
        self._reference_profile = reference_profile

    def feature_statistics(
        self, feature_name: str, profile: str = "reference", preferred_cell_height: str = None
    ) -> HTML:
        template = _get_compiled_template(self.FEATURE_STATISTICS_TEMPLATE_NAME)
        if self._reference_profile and profile.lower() == "reference":
            selected_profile = self._reference_profile._columns
        else:
            selected_profile = self._target_profile._columns  # type: ignore

        rendered_template = template(
            {
                "profile_feature_statistics_from_whylogs": json.dumps(
                    add_feature_statistics(feature_name, selected_profile)
                )
            }
        )
        return self._display_rendered_template(
            rendered_template, self.FEATURE_STATISTICS_TEMPLATE_NAME, preferred_cell_height
        )

    def write(self, rendered_html: HTML, preferred_path: str = None, html_file_name: str = None) -> None:
        if not html_file_name:
            if self._reference_profile:
                html_file_name = "todo"  # todo on v1
                # html_file_name = self._reference_profile.dataset_timestamp #v0 reference
            else:
                html_file_name = "todo"
                # html_file_name = self._target_profile.dataset_timestamp
        if preferred_path:
            path = os.path.join(os.path.expanduser(preferred_path), str(html_file_name) + ".html")
        else:
            path = os.path.join(os.pardir, "html_reports", str(html_file_name) + ".html")
        full_path = os.path.abspath(path)
        with open(full_path, "w") as saved_html:
            saved_html.write(rendered_html.data)
        saved_html.close()
