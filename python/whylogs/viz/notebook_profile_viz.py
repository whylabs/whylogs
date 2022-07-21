import html
import json
import logging
import os
from typing import Any, Dict, Optional

from IPython.core.display import HTML  # type: ignore

from whylogs.api.usage_stats import emit_usage
from whylogs.core.configs import SummaryConfig
from whylogs.core.constraints import Constraints
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.viz.enums.enums import PageSpec, PageSpecEnum
from whylogs.viz.utils.frequent_items_calculations import zero_padding_frequent_items
from whylogs.viz.utils.html_template_utils import _get_compiled_template
from whylogs.viz.utils.profile_viz_calculations import (
    add_feature_statistics,
    frequent_items_from_view,
    generate_profile_summary,
    generate_summaries,
    histogram_from_view,
)

logger = logging.getLogger(__name__)
emit_usage("visualizer")


class NotebookProfileVisualizer:
    """
    Visualize and compare profiles for drift detection, data quality, distribution comparison and feature statistics.

    NotebookProfileVisualizer enables visualization features for Jupyter Notebook environments, but also enables
    download
    of the generated reports as HTML files.

    Examples
    --------

    Create target and reference dataframes:

    .. code-block:: python

        import pandas as pd

        data_target = {
            "animal": ["cat", "hawk", "snake", "cat", "snake", "cat", "cat", "snake", "hawk","cat"],
            "legs": [4, 2, 0, 4, 0, 4, 4, 0, 2, 4],
            "weight": [4.3, None, 2.3, 7.8, 3.7, 2.5, 5.5, 3.3, 0.6, 13.3],
        }

        data_reference = {
            "animal": ["hawk", "hawk", "snake", "hawk", "snake", "snake", "cat", "snake", "hawk","snake"],
            "legs": [2, 2, 0, 2, 0, 0, 4, 0, 2, 0],
            "weight": [2.7, None, 1.2, 10.5, 2.2, 4.6, 3.8, 4.7, 0.6, 11.2],
        }

        target_df = pd.DataFrame(data_target)
        reference_df = pd.DataFrame(data_reference)

    Log data and create profile views:

    .. code-block:: python

        import whylogs as why

        results = why.log(pandas=target_df)
        prof_view = results.view()

        results_ref = why.log(pandas=reference_df)
        prof_view_ref = results_ref.view()

    Log data and create profile views:

    .. code-block:: python

        import whylogs as why

        results = why.log(pandas=target_df)
        prof_view = results.view()

        results_ref = why.log(pandas=reference_df)
        prof_view_ref = results_ref.view()

    Instantiate and set profile views:

    .. code-block:: python

        from whylogs.viz import NotebookProfileVisualizer

        visualization = NotebookProfileVisualizer()
        visualization.set_profiles(target_profile_view=prof_view,reference_profile_view=prof_view_ref)
    """

    _ref_view: Optional[DatasetProfileView]
    _target_view: DatasetProfileView

    @staticmethod
    def _display(template: str, page_spec: PageSpec, height: Optional[str]) -> "HTML":
        if not height:
            height = page_spec.height
        iframe = f"""<div></div><iframe srcdoc="{html.escape(template)}" width=100% height={height}
        frameBorder=0></iframe>"""
        display = HTML(iframe)
        return display

    def _display_distribution_chart(
        self, feature_name: str, difference: bool, cell_height: str = None, config: Optional[SummaryConfig] = None
    ) -> Optional[HTML]:
        if config is None:
            config = SummaryConfig()
        if difference:
            page_spec = PageSpecEnum.DIFFERENCED_CHART.value
        else:
            page_spec = PageSpecEnum.DISTRIBUTION_CHART.value

        template = _get_compiled_template(page_spec.html)
        if self._target_view:
            target_profile_features: Dict[str, Dict[str, Any]] = {feature_name: {}}
            reference_profile_features: Dict[str, Dict[str, Any]] = {feature_name: {}}

            target_column_profile_view = self._target_view.get_column(feature_name)
            if not target_column_profile_view:
                raise ValueError("ColumnProfileView for feature {} not found.".format(feature_name))

            target_profile_features[feature_name]["frequentItems"] = frequent_items_from_view(
                target_column_profile_view, feature_name, config
            )
            if self._ref_view:
                ref_col_view = self._ref_view.get_column(feature_name)
                if not ref_col_view:
                    raise ValueError("ColumnProfileView for feature {} not found.".format(feature_name))

                reference_profile_features[feature_name]["frequentItems"] = frequent_items_from_view(
                    ref_col_view, feature_name, config
                )

                (
                    target_profile_features[feature_name]["frequentItems"],
                    reference_profile_features[feature_name]["frequentItems"],
                ) = zero_padding_frequent_items(
                    target_feature_items=target_profile_features[feature_name]["frequentItems"],
                    reference_feature_items=reference_profile_features[feature_name]["frequentItems"],
                )
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
            result = self._display(distribution_chart, page_spec, cell_height)
            return result

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
                raise ValueError(f"ColumnProfileView for feature {feature_name} not found.")

            target_histogram = histogram_from_view(target_col_view, feature_name)
            if self._ref_view:
                reference_column_profile_view = self._ref_view.get_column(feature_name)
                if not reference_column_profile_view:
                    raise ValueError("ColumnProfileView for feature {} not found.".format(feature_name))
                ref_histogram = histogram_from_view(reference_column_profile_view, feature_name)
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

    def profile_summary(self, cell_height: str = None) -> HTML:
        page_spec = PageSpecEnum.PROFILE_SUMMARY.value
        template = _get_compiled_template(page_spec.html)

        try:
            profile_summary = generate_profile_summary(self._target_view, config=None)
            rendered_template = template(profile_summary)
            return self._display(rendered_template, page_spec, cell_height)
        except ValueError as e:
            logger.error("This method has to get target Dataset Profile View")
            raise e

    def summary_drift_report(self, height: Optional[str] = None) -> HTML:
        """Generate drift report between target and reference profiles.

        KS test is applied for continuous variables and ChiSquared test for categorical variables.
        If feature is missing from any profile, it will not be included in the report.
        Both target_profile_view and reference_profile_view must be set previously with `set_profiles`.

        Parameters
        ----------
        height: str, optional
            Preferred height, in pixels, for in-notebook visualization. Example:
            `"1000px"`. (Default is None)

        Returns
        -------
        HTML
            HTML Page of the given plot.

        Examples
        --------

        Generate Summary Drift Report (after setting profiles with `set_profiles`):

        .. code-block:: python

            visualization.summary_drift_report()

        """
        if not self._target_view or not self._ref_view:
            logger.error("This method has to get both target and reference profiles")
            raise ValueError

        page_spec = PageSpecEnum.SUMMARY_REPORT.value
        template = _get_compiled_template(page_spec.html)

        profiles_summary = generate_summaries(self._target_view, self._ref_view, config=None)
        rendered_template = template(profiles_summary)
        summary_drift_report = self._display(rendered_template, page_spec, height)
        return summary_drift_report

    def double_histogram(self, feature_name: str, cell_height: Optional[str] = None) -> HTML:
        """Plot overlayed histograms for specified feature present in both `target_profile` and `reference_profile`.

        Applicable to numerical features only.
        If reference profile was not set, `double_histogram` will plot single histogram for target profile.

        Parameters
        ----------
        feature_name: str
            Name of the feature to generate histograms.
        cell_height: str, optional
            Preferred cell height, in pixels, for in-notebook visualization. Example:
            `"1000px"`. (Default is None)

        Examples
        --------

        Generate double histogram plot for feature named `weight` (after setting profiles with `set_profiles`)

        .. code-block:: python

            visualization.double_histogram(feature_name="weight")
        """
        double_histogram = self._display_histogram_chart(feature_name, cell_height)
        return double_histogram

    def distribution_chart(self, feature_name: str, cell_height: Optional[str] = None) -> HTML:
        """Plot overlayed distribution charts for specified feature between two profiles.

        Applicable to categorical features.
        If reference profile was not set, `distribution_chart` will plot single chart for target profile.


        Parameters
        ----------
        feature_name : str
            Name of the feature to plot chart.
        cell_height : str, optional
            Preferred cell height, in pixels, for in-notebook visualization. Example:
            `cell_height="1000px"`. (Default is None)

        Returns
        -------
        HTML
            HTML Page of the given plot.

        Examples
        --------

        Generate distribution chart for `animal` feature (after setting profiles with `set_profiles`):

        .. code-block:: python

            visualization.distribution_chart(feature_name="animal")
        """
        difference = False
        distribution_chart = self._display_distribution_chart(feature_name, difference, cell_height)
        return distribution_chart

    def difference_distribution_chart(self, feature_name: str, cell_height: Optional[str] = None) -> HTML:
        """Plot overlayed distribution charts of differences between the categories of both profiles.

        Applicable to categorical features.

        Parameters
        ----------
        feature_name : str
            Name of the feature to plot chart.
        cell_height : str, optional
            Preferred cell height, in pixels, for in-notebook visualization. Example:
            `cell_height="1000px"`. (Default is None)

        Returns
        -------
        HTML
            HTML Page of the given plot.

        Examples
        --------

        Generate Difference Distribution Chart for feature named "animal":

        .. code-block:: python

            visualization.difference_distribution_chart(feature_name="animal")

        """
        difference = True
        difference_distribution_chart = self._display_distribution_chart(feature_name, difference, cell_height)
        return difference_distribution_chart

    def constraints_report(self, constraints: Constraints, cell_height: Optional[str] = None) -> HTML:
        page_spec = PageSpecEnum.CONSTRAINTS_REPORT.value
        template = _get_compiled_template(page_spec.html)
        rendered_template = template({"constraints_report": json.dumps(constraints.report())})
        constraints_report = self._display(rendered_template, page_spec, cell_height)
        return constraints_report

    def feature_statistics(
        self, feature_name: str, profile: str = "reference", cell_height: Optional[str] = None
    ) -> HTML:
        """
        Generate a report for the main statistics of specified feature, for a given profile (target or reference).

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

        Examples
        --------

        Generate Difference Distribution Chart for feature named "weight", for target profile:

        .. code-block:: python

            visualization.feature_statistics(feature_name="weight", profile="target")

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
        feature_statistics = self._display(rendered_template, page_spec, cell_height)
        return feature_statistics

    @staticmethod
    def write(
        rendered_html: HTML,
        preferred_path: Optional[str] = None,  # type: ignore
        html_file_name: Optional[str] = None,  # type: ignore
    ) -> None:
        """Create HTML file for a given report.

        Parameters
        ----------
        rendered_html: HTML, optional
            Rendered HTML returned by a given report.
        preferred_path: str, optional
            Preferred path to write the HTML file.
        html_file_name: str, optional
            Name for the created HTML file. If none is passed, created HTML will be named `ProfileVisualizer.html`

        Examples
        --------
        Dowloads an HTML page named `test.html` into the current working directory, with feature statistics for `weight` feature for the target profile.

        .. code-block:: python

            import os
            visualization.write(
                rendered_html=visualization.feature_statistics(feature_name="weight", profile="target"),
                html_file_name=os.getcwd() + "/test",
            )


        """
        if not html_file_name:
            html_file_name = "ProfileVisualizer"
        if preferred_path:
            full_path = os.path.join(os.path.expanduser(preferred_path), str(html_file_name) + ".html")
        else:
            full_path = os.path.join(os.pardir, "html_reports", str(html_file_name) + ".html")

        with open(os.path.abspath(full_path), "w") as saved_html:
            saved_html.write(rendered_html.data)
