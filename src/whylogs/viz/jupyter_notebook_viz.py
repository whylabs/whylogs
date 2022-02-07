import logging
import os
import sys
import json
import html
from typing import List
from IPython.core.display import display, HTML

from whylogs.core import DatasetProfile
from whylogs.util.protobuf import message_to_json


_MY_DIR = os.path.realpath(os.path.dirname(__file__))

logger = logging.getLogger(__name__)


class NotebookProfileViewer:
    SUMMARY_REPORT_TEMPLATE_NAME = 'index-hbs-cdn-all-in-for-jupyter-notebook.html'
    SUMMARY_STATISTICS_TEMPLATE_NAME = 'index-hbs-cdn-all-in-jupyter-full-summary-statistics.html'
    DOUBLE_HISTOGRAM_TEMPLATE_NAME = 'index-hbs-cdn-all-in-jupyter-distribution-chart.html'
    FEATURE_STATISTICS_TEMPLATE_NAME = 'index-hbs-cdn-all-in-jupyter-feature-summary-statistics.html'
    PAGE_SIZES = {
        SUMMARY_REPORT_TEMPLATE_NAME: '1000px',
        DOUBLE_HISTOGRAM_TEMPLATE_NAME: '277px',
        SUMMARY_STATISTICS_TEMPLATE_NAME: '250px',
        FEATURE_STATISTICS_TEMPLATE_NAME: '650px',
    }

    def __init__(self, target_profiles: List[DatasetProfile] = None, reference_profiles: List[DatasetProfile] = None):
        self.target_profiles = target_profiles
        self.reference_profiles = reference_profiles
        # create json output from profiles
        if self.target_profiles:
            if len(self.target_profiles) > 1:
                logger.warning(
                    "More than one profile not implemented yet, default to first profile in the list "
                )
            self.target_profile_jsons = [
                message_to_json(each_prof.to_summary()) for each_prof in self.target_profiles
            ]
            if self.reference_profiles:
                self.reference_profile_jsons = [
                    message_to_json(each_prof.to_summary()) for each_prof in self.reference_profiles
                ]
        else:
            logger.warning(
                "Got no profile data, make sure you pass data correctly ")
            return None

    def __get_template_path(self, html_file_name):
        template_path = os.path.abspath(
            os.path.join(
                _MY_DIR, os.pardir, "viewer", html_file_name
            )
        )
        return template_path

    def __get_compiled_template(self, template_name):
        template_path = self.__get_template_path(template_name)
        try:
            from pybars import Compiler
        except ImportError as e:
            Compiler = None
            logger.debug(str(e))
            logger.debug(
                "Unable to load pybars; install pybars3 to load profile from directly from the current session "
            )
        with open(template_path, "r") as file_with_template:
            source = file_with_template.read()
        # compile templated files
        compiler = Compiler()
        template = compiler.compile(source)
        return template

    def __pull_feature_data(self, profile_jsons, feature_name):
        profile_features = json.loads(profile_jsons[0])
        feature_data = {}
        feature_data['properties'] = profile_features.get('properties')
        feature_data[feature_name] = profile_features.get('columns').get(feature_name)
        return feature_data

    def __display_rendered_template(self, template, template_name, height):
        if not height:
            height = self.PAGE_SIZES[template_name]
        # convert html to iframe and return it wrapped in Ipython...HTML()
        iframe = f'''<iframe srcdoc="{html.escape(template)}" width=100% height={height} frameBorder=0></iframe>'''
        return HTML(iframe)

    def summary_drift_report(self, preferred_cell_height=None):
        template = self.__get_compiled_template(self.SUMMARY_REPORT_TEMPLATE_NAME)
        profiles_summary = {"profile_from_whylogs": self.target_profile_jsons[0]}
        if self.reference_profiles:
            profiles_summary["reference_profile_from_whylogs"] = self.reference_profile_jsons[0]
        return self.__display_rendered_template(
            template(profiles_summary),
            self.SUMMARY_REPORT_TEMPLATE_NAME,
            preferred_cell_height
        )

    def double_histogram(self, feature_names, preferred_cell_height=None):
        if type(feature_names) is not list:
            feature_names = [feature_names]
        template = self.__get_compiled_template(self.DOUBLE_HISTOGRAM_TEMPLATE_NAME)
        if self.reference_profiles:
            target_profile_columns = json.loads(self.target_profile_jsons[0]).get('columns')
            reference_profile_columns = json.loads(self.reference_profile_jsons[0]).get('columns')
            target_profile_features, reference_profile_features = {}, {}
            for feature_name in feature_names:
                target_profile_features[feature_name] = target_profile_columns.get(feature_name)
                reference_profile_features[feature_name] = reference_profile_columns.get(
                    feature_name
                )
            distribution_chart = template({
                "profile_from_whylogs": json.dumps(target_profile_features),
                "reference_profile_from_whylogs": json.dumps(reference_profile_features)
            })
            return self.__display_rendered_template(
                distribution_chart,
                self.DOUBLE_HISTOGRAM_TEMPLATE_NAME,
                preferred_cell_height
            )
        else:
            logger.warning(
                "This method has to get both target and reference profiles, with valid feature title"
            )
            return None

    def summary_statistics(self, profile='reference', preferred_cell_height=None):
        template = self.__get_compiled_template(self.SUMMARY_STATISTICS_TEMPLATE_NAME)
        if self.reference_profiles and profile.lower() == 'reference':
            profile_statistics = self.reference_profile_jsons[0]
        else:
            profile_statistics = self.target_profile_jsons[0]
        rendered_template = template({
            "profile_summary_statistics_from_whylogs": profile_statistics
        })
        return self.__display_rendered_template(
            rendered_template,
            self.SUMMARY_STATISTICS_TEMPLATE_NAME,
            preferred_cell_height
        )

    def feature_summary_statistics(self, feature_name, profile='reference', preferred_cell_height=None):
        template = self.__get_compiled_template(self.FEATURE_STATISTICS_TEMPLATE_NAME)
        if self.reference_profiles and profile.lower() == 'reference':
            selected_profile = self.reference_profile_jsons
        else:
            selected_profile = self.target_profile_jsons
        rendered_template = template({
            "profile_feature_summary_statistics_from_whylogs": json.dumps(
                self.__pull_feature_data(selected_profile, feature_name)
            )}
        )
        return self.__display_rendered_template(
            rendered_template,
            self.FEATURE_STATISTICS_TEMPLATE_NAME,
            preferred_cell_height
        )

    def download(self, html, preferred_path=None, html_file_name=None):
        if not html_file_name:
            if self.reference_profiles:
                html_file_name = self.reference_profiles[0].dataset_timestamp
            else:
                html_file_name = self.target_profiles[0].dataset_timestamp
        if preferred_path:
            path = os.path.expanduser(preferred_path)
        else:
            path = os.path.join(os.pardir, "html_reports", str(html_file_name)+".html")
        full_path = os.path.abspath(path)
        with open(full_path, "w") as saved_html:
            saved_html.write(html.data)
        saved_html.close()
        return None
