import html
import json
import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional

from IPython.core.display import HTML  # type: ignore

from whylogs.api.usage_stats import emit_usage
from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics import DistributionMetric
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.viz.utils.profile_viz_calculations import (
    add_feature_statistics,
    get_frequent_items_estimate,
    histogram_from_sketch,
)

logger = logging.getLogger(__name__)
_MY_DIR = os.path.realpath(os.path.dirname(__file__))
emit_usage("visualizer")


def _get_template_path(html_file_name: str) -> str:
    template_path = os.path.abspath(os.path.join(_MY_DIR, "html", "templates", html_file_name))
    return template_path


def _get_compiled_template(template_name: str) -> "Callable":
    template_path = _get_template_path(template_name)
    try:
        from pybars import Compiler  # type: ignore
    except ImportError as e:
        logger.debug(e, exc_info=True)
        logger.warning("Unable to load pybars; install pybars3 to load profile directly from the current session ")
        return lambda _: None  # returns a no-op lambda

    with open(template_path, "r+t") as file_with_template:
        source = file_with_template.read()
    return Compiler().compile(source)


@dataclass
class PageSpec:
    html: str
    height: str


class PageSpecEnum(Enum):
    SUMMARY_REPORT = PageSpec(html="index-hbs-cdn-all-in-for-jupyter-notebook.html", height="1000px")
    DOUBLE_HISTOGRAM = PageSpec(html="index-hbs-cdn-all-in-jupyter-distribution-chart.html", height="300px")
    DISTRIBUTION_CHART = PageSpec(html="index-hbs-cdn-all-in-jupyter-bar-chart.html", height="277px")
    DIFFERENCED_CHART = PageSpec(html="index-hbs-cdn-all-in-jupyter-differenced-chart.html.html", height="277px")
    FEATURE_STATISTICS = PageSpec(html="index-hbs-cdn-all-in-jupyter-feature-summary-statistics.html", height="650px")
    CONSTRAINTS_REPORT = PageSpec(html="index-hbs-cdn-all-in-jupyter-constraints-report.html", height="750px")


class NotebookProfileVisualizer:
    """
    Visualize and compare profiles for drift detection, data quality, distribution comparison and feature statistics.

    NotebookProfileVisualizer enables visualization features for Jupyter Notebook environments, but also enables
    download
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

    _ref_view: Optional[DatasetProfileView]
    _target_view: DatasetProfileView

    @staticmethod
    def _display(template: str, page_spec: PageSpec, height: Optional[str]) -> HTML:
        if not height:
            height = page_spec.height
        iframe = f"""<div></div><iframe srcdoc="{html.escape(template)}" width=100% height={height}
        frameBorder=0></iframe>"""
        display = HTML(iframe)
        return display

    def _display_distribution_chart(
        self, feature_name: str, cell_height: str = None, config: Optional[SummaryConfig] = None
    ) -> Optional[HTML]:
        if config is None:
            config = SummaryConfig()
        page_spec = PageSpecEnum.DISTRIBUTION_CHART.value

        template = _get_compiled_template(page_spec.html)
        if self._target_view:
            target_profile_features: Dict[str, Dict[str, Any]] = {feature_name: {}}
            reference_profile_features: Dict[str, Dict[str, Any]] = {feature_name: {}}

            target_column_profile_view = self._target_view.get_column(feature_name)
            if not target_column_profile_view:
                raise ValueError("ColumnProfileView for feature {} not found.".format(feature_name))

            target_column_frequent_items_metric = target_column_profile_view.get_metric("fi")
            if not target_column_frequent_items_metric:
                raise ValueError("Frequent Items Metrics not found for feature {}.".format(feature_name))

            target_frequent_items = target_column_frequent_items_metric.to_summary_dict(config)["fs"]
            target_profile_features[feature_name]["frequentItems"] = get_frequent_items_estimate(target_frequent_items)
            if self._ref_view:
                ref_col_view = self._ref_view.get_column(feature_name)
                if not ref_col_view:
                    raise ValueError("ColumnProfileView for feature {} not found.".format(feature_name))

                ref_col_freq_item_metric = ref_col_view.get_metric("fi")
                if not ref_col_freq_item_metric:
                    raise ValueError("Frequent Items Metrics not found for feature {}.".format(feature_name))

                ref_freq_items = ref_col_freq_item_metric.to_summary_dict(config)["fs"]
                reference_profile_features[feature_name]["frequentItems"] = get_frequent_items_estimate(ref_freq_items)
            else:
                logger.warning("Reference profile not detected. Plotting only for target feature.")
                reference_profile_features[feature_name]["frequentItems"] = [
                    {"value": x["value"], "estimate": 0} for x in target_profile_features[feature_name]["frequentItems"]
                ]  # Getting the same frequent items categories for target and adding 0 as estimate.
            distribution_chart = template(
                {
                    "profile_from_whylogs": json.dumps(target_profile_features),
                    "reference_profile_from_whylogs": json.dumps(reference_profile_features),
                }
            )
            return self._display(distribution_chart, page_spec, cell_height)

        else:
            logger.warning("This method has to get at least a target profile, with valid feature title")
            return None

    def _display_histogram_chart(self, feature_name: str, cell_height: str = None) -> Optional[HTML]:
        page_spec = PageSpecEnum.DOUBLE_HISTOGRAM.value
        template = _get_compiled_template(page_spec.html)
        if self._target_view:
            target_features: Dict[str, Dict[str, Any]] = {feature_name: {}}
            ref_features: Dict[str, Dict[str, Any]] = {feature_name: {}}

            target_col_view = self._target_view.get_column(feature_name)
            if not target_col_view:
                raise ValueError("ColumnProfileView for feature {} not found.".format(feature_name))

            target_col_dist: Optional[DistributionMetric] = target_col_view.get_metric("dist")
            if not target_col_dist:
                raise ValueError("Distribution Metrics not found for feature {}.".format(feature_name))

            target_kill = target_col_dist.kll.value
            target_histogram = histogram_from_sketch(target_kill)
            if self._ref_view:
                reference_column_profile_view = self._ref_view.get_column(feature_name)
                if not reference_column_profile_view:
                    raise ValueError("ColumnProfileView for feature {} not found.".format(feature_name))

                reference_column_dist_metric = reference_column_profile_view.get_metric("dist")
                if not reference_column_dist_metric:
                    raise ValueError("Distribution Metrics not found for feature {}.".format(feature_name))

                reference_kll_sketch = reference_column_dist_metric.kll.value
                ref_histogram = histogram_from_sketch(reference_kll_sketch)
            else:
                logger.warning("Reference profile not detected. Plotting only for target feature.")
                ref_histogram = target_histogram.copy()
                ref_histogram["counts"] = [
                    0 for _ in ref_histogram["counts"]
                ]  # To plot single profile, zero counts for non-existing profile.

            ref_features[feature_name]["histogram"] = ref_histogram
            target_features[feature_name]["histogram"] = target_histogram
            histogram_chart = template(
                {
                    "profile_from_whylogs": json.dumps(target_features),
                    "reference_profile_from_whylogs": json.dumps(ref_features),
                }
            )
            return self._display(histogram_chart, page_spec, height=cell_height)
        else:
            logger.warning("This method has to get at least a target profile, with valid feature title")
            return None

    def set_profiles(
        self, target_profile_view: DatasetProfileView, reference_profile_view: Optional[DatasetProfileView] = None
    ) -> None:
        """Set profiles for Visualization/Comparison.

        Drift calculation is done if both `target_profile` and `reference profile` are passed.

        Parameters
        ----------
        target_profile_view: DatasetProfileView, required
            Target profile to visualize.
        reference_profile_view: DatasetProfileView, optional
            Reference, or baseline, profile to be compared against the target profile.

        """
        self._target_view = target_profile_view
        self._ref_view = reference_profile_view

    def double_histogram(self, feature_name: str, cell_height: str = None) -> HTML:
        """Plots overlayed histograms for specified feature present in both `target_profile` and `reference_profile`.
        Applicable to numerical features only.

        Parameters
        ----------
        feature_name: str
            Name of the feature to generate histograms.
        cell_height: str, optional
            Preferred cell height, in pixels, for in-notebook visualization. Example:
            `cell_height="1000px"`. (Default is None)

        """
        return self._display_histogram_chart(feature_name, cell_height)

    def distribution_chart(self, feature_name: str, cell_height: str = None) -> HTML:
        return self._display_distribution_chart(feature_name, cell_height)

    def feature_statistics(self, feature_name: str, profile: str = "reference", cell_height: str = None) -> HTML:
        """
        Generates a report for the main statistics of specified feature, for a given profile (target or reference).

        Statistics include overall metrics such as distinct and missing values, as well as quantile and descriptive
        statistics.
        If `profile` is not passed, the default is the reference profile.

        Parameters
        ----------
        feature_name: str
            Name of the feature to generate histograms.
        profile: str
            Profile to be used to generate the report. (Default is `reference`)
        cell_height: str, optional
            Preferred cell height, in pixels, for in-notebook visualization. Example:
            `cell_height="1000px"`. (Default is None)

        """
        page_spec = PageSpecEnum.FEATURE_STATISTICS.value
        template = _get_compiled_template(page_spec.html)
        if self._ref_view and profile.lower() == "reference":
            selected_profile_column = self._ref_view.get_column(feature_name)
        else:
            selected_profile_column = self._target_view.get_column(feature_name)

        rendered_template = template(
            {
                "profile_feature_statistics_from_whylogs": json.dumps(
                    add_feature_statistics(feature_name, selected_profile_column)
                )
            }
        )
        return self._display(rendered_template, page_spec, cell_height)

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
        >>> from whylogs.viz import NotebookProfileVisualizer
        >>>
        >>> visualization = NotebookProfileVisualizer()
        >>> visualization.write(
        ...    rendered_html=visualization.feature_statistics(feature_name="weight", profile="target"),
        ...    html_file_name=os.getcwd() + "/test",
        ... )


        """
        if not html_file_name:
            if self._ref_view:
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
