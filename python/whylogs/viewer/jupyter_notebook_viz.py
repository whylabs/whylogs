import html
import json
import logging
import os
from typing import Any, Dict, Optional

from IPython.core.display import HTML  # type: ignore

from whylogs.core.view.dataset_profile_view import DatasetProfileView

from .utils.profile_viz_calculations import (
    add_feature_statistics,
    histogram_from_sketch,
)

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
    """
    Visualize and compare profiles for drift detection, data quality, distribution comparison and feature statistics.

    NotebookProfileVisualizer enables visualization features for Jupyter Notebook environments, but also enables download
    of the generated reports as HTML files.

    Examples
    --------
    .. code-block:: python

        data = {
            "animal": ["cat", "hawk", "snake", "cat"],
            "legs": [4, 2, 0, 4],
            "weight": [4.3, 1.8, None, 4.1],
        }

        df = pd.DataFrame(data)

        results = ylog.log(pandas=df)
        profile = results.get_profile()
        profile_view = profile.view()
        visualization = NotebookProfileVisualizer()
        visualization.set_profiles(target_profile_view=profile_view, reference_profile_view=profile_view)


    """

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

    def _display_feature_chart(
        self, feature_name: str, template_name: str, preferred_cell_height: str = None
    ) -> Optional[HTML]:
        template = _get_compiled_template(template_name)
        if self._target_profile_view:
            target_profile_features: Dict[str, Dict[str, Any]] = {feature_name: {}}
            reference_profile_features: Dict[str, Dict[str, Any]] = {feature_name: {}}

            target_column_profile_view = self._target_profile_view.get_column(feature_name)
            if not target_column_profile_view:
                raise ValueError("ColumnProfileView for feature {} not found.".format(feature_name))

            target_column_dist_metric = target_column_profile_view.get_metric("dist")
            if not target_column_dist_metric:
                raise ValueError("Distribution Metrics not found for feature {}.".format(feature_name))

            target_kll_sketch = target_column_dist_metric.kll.value  # type: ignore
            target_histogram = histogram_from_sketch(target_kll_sketch)
            if self._reference_profile_view:
                reference_column_profile_view = self._reference_profile_view.get_column(feature_name)
                if not reference_column_profile_view:
                    raise ValueError("ColumnProfileView for feature {} not found.".format(feature_name))

                reference_column_dist_metric = reference_column_profile_view.get_metric("dist")
                if not reference_column_dist_metric:
                    raise ValueError("Distribution Metrics not found for feature {}.".format(feature_name))

                reference_kll_sketch = reference_column_dist_metric.kll.value  # type: ignore
                reference_histogram = histogram_from_sketch(reference_kll_sketch)
            else:
                logger.warning("Reference profile not detected. Plotting only for target feature.")
                reference_histogram = target_histogram.copy()
                reference_histogram["counts"] = [
                    0 for x in reference_histogram["counts"]
                ]  # To plot single profile, zero counts for non-existing profile.

            reference_profile_features[feature_name]["histogram"] = reference_histogram
            target_profile_features[feature_name]["histogram"] = target_histogram
            distribution_chart = template(
                {
                    "profile_from_whylogs": json.dumps(target_profile_features),
                    "reference_profile_from_whylogs": json.dumps(reference_profile_features),
                }
            )
            return self._display_rendered_template(distribution_chart, template_name, preferred_cell_height)
        else:
            logger.warning("This method has to get at least a target profile, with valid feature title")
            return None

    def set_profiles(
        self, target_profile_view: DatasetProfileView, reference_profile_view: DatasetProfileView = None
    ) -> None:
        """Set profiles for Visualization/Comparison.

        Drift calculation is done if both `target_profile` and `reference profile` are passed.

        Parameters
        ----------
        target_profile_view: DatasetProfileView, optional
            Target profile to visualize.
        reference_profile_view: DatasetProfileView, optional
            Reference, or baseline, profile to be compared against the target profile.

        """
        self._target_profile_view = target_profile_view
        self._reference_profile_view = reference_profile_view

    def double_histogram(self, feature_name: str, preferred_cell_height: str = None) -> HTML:
        """Plots overlayed histograms for specified feature present in both `target_profile` and `reference_profile`. Applicable to numerical features only.

        Parameters
        ----------
        feature_name: str
            Name of the feature to generate histograms.
        preferred_cell_height: str, optional
            Preferred cell height, in pixels, for in-notebook visualization. Example: `preferred_cell_height="1000px"`. (Default is None)

        """
        return self._display_feature_chart(feature_name, self.DOUBLE_HISTOGRAM_TEMPLATE_NAME, preferred_cell_height)

    def feature_statistics(
        self, feature_name: str, profile: str = "reference", preferred_cell_height: str = None
    ) -> HTML:
        """
        Generates a report for the main statistics of specified feature, for a given profile (target or reference).

        Statistics include overall metrics such as distinct and missing values, as well as quantile and descriptive statistics.
        If `profile` is not passed, the default is the reference profile.

        Parameters
        ----------
        feature_name: str
            Name of the feature to generate histograms.
        profile: str
            Profile to be used to generate the report. (Default is `reference`)
        preferred_cell_height: str, optional
            Preferred cell height, in pixels, for in-notebook visualization. Example: `preferred_cell_height="1000px"`. (Default is None)

        """
        template = _get_compiled_template(self.FEATURE_STATISTICS_TEMPLATE_NAME)
        if self._reference_profile_view and profile.lower() == "reference":
            selected_profile_column = self._reference_profile_view.get_column(feature_name)
        else:
            selected_profile_column = self._target_profile_view.get_column(feature_name)  # type: ignore

        rendered_template = template(
            {
                "profile_feature_statistics_from_whylogs": json.dumps(
                    add_feature_statistics(feature_name, selected_profile_column)
                )
            }
        )
        return self._display_rendered_template(
            rendered_template, self.FEATURE_STATISTICS_TEMPLATE_NAME, preferred_cell_height
        )

    def write(self, rendered_html: HTML, preferred_path: str = None, html_file_name: str = None) -> None:
        """Creates HTML file for the given report.

        Parameters
        ----------
        rendered_html: HTML
            Rendered HTML returned by a given report.
        preferred_path: str, optional
            Preferred path to write the HTML file.
        html_file_name: str, optional
            Name for the created HTML file. If none is passed, created HTML will be named `ProfileVisualizer.html`

        Examples
        --------
        >>> import os
        >>> visualization.write(
        ...    rendered_html=visualization.feature_statistics(feature_name="weight", profile="target"),
        ...    html_file_name=os.getcwd() + "/test",
        ... )


        """
        if not html_file_name:
            if self._reference_profile_view:
                html_file_name = "ProfileVisualizer"  # todo on v1
                # html_file_name = self._reference_profile.dataset_timestamp #v0 reference
            else:
                html_file_name = "ProfileVisualizer"
                # html_file_name = self._target_profile.dataset_timestamp
        if preferred_path:
            path = os.path.join(os.path.expanduser(preferred_path), str(html_file_name) + ".html")
        else:
            path = os.path.join(os.pardir, "html_reports", str(html_file_name) + ".html")
        full_path = os.path.abspath(path)
        with open(full_path, "w") as saved_html:
            saved_html.write(rendered_html.data)
        saved_html.close()
