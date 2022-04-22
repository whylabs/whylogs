import html
import json
import logging
import os

from IPython.core.display import HTML

from whylogs.core import DatasetProfile
from whylogs.proto import InferredType
from whylogs.util.protobuf import message_to_json

from .utils.profile_viz_calculations import (
    add_drift_val_to_ref_profile_json,
    add_feature_statistics,
)

_MY_DIR = os.path.realpath(os.path.dirname(__file__))

logger = logging.getLogger(__name__)

numerical_types = (InferredType.Type.INTEGRAL, InferredType.Type.FRACTIONAL)


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

    def __get_template_path(self, html_file_name):
        template_path = os.path.abspath(os.path.join(_MY_DIR, os.pardir, "viewer/templates", html_file_name))
        return template_path

    def __get_compiled_template(self, template_name):
        template_path = self.__get_template_path(template_name)
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

    def __display_feature_chart(self, feature_names, template_name, preferred_cell_height=None):
        if type(feature_names) is not list:
            feature_names = [feature_names]
        template = self.__get_compiled_template(template_name)
        if self._reference_profile:
            target_profile_columns = json.loads(self._target_profile_json).get("columns")
            reference_profile_columns = json.loads(self._reference_profile_json).get("columns")
            target_profile_features, reference_profile_features = {}, {}
            for feature_name in feature_names:
                target_profile_features[feature_name] = target_profile_columns.get(feature_name)
                reference_profile_features[feature_name] = reference_profile_columns.get(feature_name)
            distribution_chart = template(
                {"profile_from_whylogs": json.dumps(target_profile_features), "reference_profile_from_whylogs": json.dumps(reference_profile_features)}
            )
            return self.__display_rendered_template(distribution_chart, template_name, preferred_cell_height)
        else:
            logger.warning("This method has to get both target and reference profiles, with valid feature title")
            return None

    def __display_rendered_template(self, template, template_name, height):
        if not height:
            height = self.PAGE_SIZES[template_name]
        iframe = f"""<div></div><iframe srcdoc="{html.escape(template)}" width=100% height={height} frameBorder=0></iframe>"""
        return HTML(iframe)

    def set_profiles(self, target_profile: DatasetProfile = None, reference_profile: DatasetProfile = None):
        self._target_profile = target_profile
        self._reference_profile = reference_profile
        if self._target_profile:
            self._target_profile_json = message_to_json(self._target_profile.to_summary())
            if self._reference_profile:
                self._reference_profile_json = message_to_json(self._reference_profile.to_summary())

    def summary_drift_report(self, preferred_cell_height=None):
        reference_profile = add_drift_val_to_ref_profile_json(self._target_profile, self._reference_profile, json.loads(self._reference_profile_json))
        template = self.__get_compiled_template(self.SUMMARY_REPORT_TEMPLATE_NAME)
        profiles_summary = {"profile_from_whylogs": self._target_profile_json}
        if self._reference_profile:
            profiles_summary["reference_profile_from_whylogs"] = json.dumps(reference_profile)
        return self.__display_rendered_template(template(profiles_summary), self.SUMMARY_REPORT_TEMPLATE_NAME, preferred_cell_height)

    def double_histogram(self, feature_names, preferred_cell_height=None):
        return self.__display_feature_chart(feature_names, self.DOUBLE_HISTOGRAM_TEMPLATE_NAME, preferred_cell_height)

    def distribution_chart(self, feature_names, preferred_cell_height=None):
        return self.__display_feature_chart(feature_names, self.DISTRIBUTION_CHART_TEMPLATE_NAME, preferred_cell_height)

    def difference_distribution_chart(self, feature_names, preferred_cell_height=None):
        return self.__display_feature_chart(feature_names, self.DIFFERENCED_CHART_TEMPLATE_NAME, preferred_cell_height)

    def feature_statistics(self, feature_name, profile="reference", preferred_cell_height=None):
        template = self.__get_compiled_template(self.FEATURE_STATISTICS_TEMPLATE_NAME)
        if self._reference_profile and profile.lower() == "reference":
            selected_profile_json = self._reference_profile_json
            selected_profile = self._reference_profile.columns
        else:
            selected_profile_json = self._target_profile_json
            selected_profile = self._target_profile.columns
        if selected_profile.get(feature_name).schema_tracker.to_summary().inferred_type.type in numerical_types:
            rendered_template = template(
                {
                    "profile_feature_statistics_from_whylogs": json.dumps(
                        add_feature_statistics(selected_profile.get(feature_name), selected_profile_json, feature_name)
                    )
                }
            )
            return self.__display_rendered_template(rendered_template, self.FEATURE_STATISTICS_TEMPLATE_NAME, preferred_cell_height)
        else:
            logger.warning("Quantile and descriptive statistics can be calculated for numerical features only!")
            return None

    def constraints_report(self, constraints, preferred_cell_height=None):
        template = self.__get_compiled_template(self.CONSTRAINTS_REPORT_TEMPLATE_NAME)
        rendered_template = template({"constraints_report": json.dumps(constraints.report())})
        return self.__display_rendered_template(rendered_template, self.CONSTRAINTS_REPORT_TEMPLATE_NAME, preferred_cell_height)

    def download(self, html, preferred_path=None, html_file_name=None):
        if not html_file_name:
            if self._reference_profile:
                html_file_name = self._reference_profile.dataset_timestamp
            else:
                html_file_name = self._target_profile.dataset_timestamp
        if preferred_path:
            path = os.path.join(os.path.expanduser(preferred_path), str(html_file_name) + ".html")
        else:
            path = os.path.join(os.pardir, "html_reports", str(html_file_name) + ".html")
        full_path = os.path.abspath(path)
        with open(full_path, "w") as saved_html:
            saved_html.write(html.data)
        saved_html.close()
        return None
