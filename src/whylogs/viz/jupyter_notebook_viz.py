import logging
import os
import tempfile
import webbrowser
import sys
import json
import html
from typing import List
from IPython.core.display import display, HTML

from whylogs.core import DatasetProfile
from whylogs.util.protobuf import message_to_json


_MY_DIR = os.path.realpath(os.path.dirname(__file__))

logger = logging.getLogger(__name__)


class DisplayProfile:

    def __init__(self, profiles: List[DatasetProfile] = None, reference_profiles: List[DatasetProfile] = None):
        self.profiles = profiles
        self.reference_profiles = reference_profiles
        # create json output from profiles
        if self.profiles:
            if len(self.profiles) > 1:
                logger.warning(
                    "More than one profile not implemented yet, default to first profile in the list "
                )
            self.profile_jsons = [message_to_json(each_prof.to_summary())
                                  for each_prof in self.profiles]
            if self.reference_profiles:
                self.reference_profile_jsons = [message_to_json(each_prof.to_summary())
                                                for each_prof in self.reference_profiles]

        else:
            logger.warning(
                "Got no profile data, make sure you pass data correctly ")
            return None

    def __display_html(self, output_index, height):
        # convert html to iframe and return it wrapped in Ipython...HTML()
        iframe = f'''<iframe srcdoc="{html.escape(output_index)}" width=100% {height} frameBorder=0></iframe>'''
        return HTML(iframe)

    def __iframe_output_height(self, html_frame_height):
        # add all required heights and widths for individual HTMLs to be displayed in notebook
        sizes = {'index-hbs-cdn-all-in-for-jupyter-notebook.html': 'height=1000px',
                 'index-hbs-cdn-all-in-jupyter-distribution-chart.html': 'height=277px',
                 'index-hbs-cdn-all-in-jupyter-full-summary-statistics.html': 'height=250px',
                 'index-hbs-cdn-all-in-jupyter-feature-summary-statistics.html': 'height=650px',
                 }
        return str(sizes.get(html_frame_height))

    def __html_template_data(self, index_path):
        # bind profile jsons to html template
        try:
            from pybars import Compiler
        except ImportError as e:
            Compiler = None
            logger.debug(str(e))
            logger.debug(
                "Unable to load pybars; install pybars3 to load profile from directly from the current session "
            )
        with open(index_path, "r") as file_with_template:
            source = file_with_template.read()
        # compile templated files
        compiler = Compiler()
        template = compiler.compile(source)
        return template

    def __extract_feature_data(self, profile_jsons, feature_name):
        profile_features = json.loads(profile_jsons[0])
        feature_data = {}
        feature_data['properties'] = profile_features.get('properties')
        feature_data[feature_name] = profile_features.get('columns').get(feature_name)
        return feature_data

    def summary(self):
        index_path = os.path.abspath(os.path.join(
            _MY_DIR, os.pardir, "viewer",
            "index-hbs-cdn-all-in-for-jupyter-notebook.html")
        )
        html_frame_height = self.__iframe_output_height(
            "index-hbs-cdn-all-in-for-jupyter-notebook.html")
        template = self.__html_template_data(index_path)
        if self.reference_profiles:
            output_index = template(
                {"profile_from_whylogs": self.profile_jsons[0],
                 "reference_profile": self.reference_profile_jsons[0]}
            )
        else:
            output_index = template(
                {"profile_from_whylogs": self.profile_jsons[0]}
            )
        return self.__display_html(output_index, html_frame_height)

    def download(self, html, path=None, html_file_name=None):
        # code to write html arg to file and generate name using TimeStamp
        if path:
            output_path = os.path.abspath(os.path.expanduser(path))
        else:
            output_path = os.path.abspath(
                os.path.join(
                    os.pardir, "html_reports"
                )
            )
        data_timestamp = ''
        if html_file_name:
            file_name = html_file_name
        elif self.reference_profiles:
            data_timestamp = self.reference_profiles[0].dataset_timestamp
        else:
            data_timestamp = self.profiles[0].dataset_timestamp

        full_path = os.path.join(output_path, str(data_timestamp)+".html")
        with open(full_path, "w") as saved_html:
            saved_html.write(html.data)
        saved_html.close()

    def feature(self, names):
        index_path = os.path.abspath(os.path.join(
            _MY_DIR, os.pardir, "viewer",
            "index-hbs-cdn-all-in-jupyter-distribution-chart.html")
        )

        html_frame_height = self.__iframe_output_height(
            "index-hbs-cdn-all-in-jupyter-distribution-chart.html"
        )
        template = self.__html_template_data(index_path)
        # replace handlebars for json profiles
        if self.reference_profiles:
            profile_feature = json.loads(self.profile_jsons[0])
            reference_profile_feature = json.loads(self.reference_profile_jsons[0])
            profile_from_whylogs = {}
            reference_profile = {}
            for name in names:
                profile_from_whylogs[name] = profile_feature.get('columns').get(name)
                reference_profile[name] = reference_profile_feature.get('columns').get(name)
            output_index = template(
                {"profile_from_whylogs": json.dumps(profile_from_whylogs),
                 "reference_profile": json.dumps(reference_profile)}
            )
            return self.__display_html(output_index, html_frame_height)
        else:
            logger.warning(
                "This method has to get both target and reference profiles, with valid feature title"
            )
            return None

    def summary_statistics(self, profile):
        index_path = os.path.abspath(os.path.join(
            _MY_DIR, os.pardir, "viewer",
            "index-hbs-cdn-all-in-jupyter-full-summary-statistics.html")
        )
        html_frame_height = self.__iframe_output_height(
            "index-hbs-cdn-all-in-jupyter-full-summary-statistics.html")
        template = self.__html_template_data(index_path)
        if self.reference_profiles and profile == 'Reference':
            output_index = template(
                {"reference_profile": self.reference_profile_jsons[0]}
            )
        elif profile == 'Target':
            output_index = template(
                {"profile_from_whylogs": self.profile_jsons[0]}
            )
        else:
            logger.warning(
                "Please select from available options, 'Target' or 'Reference'"
            )
        return self.__display_html(output_index, html_frame_height)

    def feature_summary_statistics(self, feature_name, profile):
        index_path = os.path.abspath(os.path.join(
            _MY_DIR, os.pardir, "viewer",
            "index-hbs-cdn-all-in-jupyter-feature-summary-statistics.html")
        )

        html_frame_height = self.__iframe_output_height(
            "index-hbs-cdn-all-in-jupyter-feature-summary-statistics.html"
        )
        template = self.__html_template_data(index_path)
        # replace handlebars for json profiles
        if self.reference_profiles and profile == 'Reference':
            output_index = template(
                {
                    "reference_profile": json.dumps(
                        __extract_feature_data(
                            self, self.reference_profile_jsons, feature_name
                        )
                    )
                }
            )
            return self.__display_html(output_index, html_frame_height)
        elif self.profiles and profile == 'Target':
            output_index = template(
                {
                    "profile_from_whylogs": json.dumps(
                        __extract_feature_data(
                            self, self.profile_jsons, feature_name
                        )
                    )
                }
            )
            return self.__display_html(output_index, html_frame_height)
        else:
            logger.warning(
                "Make sure you have profile logged in and pass a valid feature name"
            )
            return None
