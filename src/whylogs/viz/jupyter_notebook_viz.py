import logging
import os
import tempfile
import webbrowser
import sys
import json
from typing import List
import html

from IPython.core.display import display, HTML
from whylogs.core import DatasetProfile
from whylogs.util.protobuf import message_to_json


_MY_DIR = os.path.realpath(os.path.dirname(__file__))

logger = logging.getLogger(__name__)


class DisplayProfile:

    def __init__(self, profiles: List[DatasetProfile] = None, reference_profiles: List[DatasetProfile] = None):

        self.profiles = profiles
        self.reference_profiles = reference_profiles

    def __display_html(self, output_index, width_height):
        # convert html to iframe and return it wrapped in Ipython...HTML()
        iframe = f'''<iframe srcdoc="{html.escape(output_index)}" {width_height} frameBorder=0></iframe>'''
        return HTML(iframe)

    def __iframe_output_sizing(self, html):
        # add all required heights and widths for individual HTMLs to be displayed in notebook
        sizes = {'index-hbs-cdn-all-in-for-jupyter-notebook.html': 'width=960px height=1000px',
                 'index-hbs-cdn-all-in-jupyter-distribution-chart.html': 'width=960px height=277px'
                 }
        return str(sizes.get(html))

    def summary(self) -> str:
        """
        create profile html
        """
        try:
            from pybars import Compiler
        except ImportError as e:
            Compiler = None
            logger.debug(str(e))
            logger.debug(
                "Unable to load pybars; install pybars3 to load profile from directly from the current session ")
        # create json output from profiles
        if self.profiles:
            if len(self.profiles) > 1:
                logger.warning(
                    "More than one profile not implemented yet, default to first profile in the list ")
            profile_jsons = [message_to_json(each_prof.to_summary())
                             for each_prof in self.profiles]

            if self.reference_profiles:
                reference_profile_jsons = [message_to_json(each_prof.to_summary())
                                           for each_prof in self.reference_profiles]

        else:
            logger.warning(
                "Got no profile data, make sure you pass data correctly ")
            return None
        index_path = os.path.abspath(os.path.join(
            _MY_DIR, os.pardir, "viewer", "index-hbs-cdn-all-in-for-jupyter-notebook.html"))

        html_file = self.__iframe_output_sizing("index-hbs-cdn-all-in-for-jupyter-notebook.html")
        with open(index_path, "r") as file_with_template:
            source = file_with_template.read()
        # compile templated files
        compiler = Compiler()
        template = compiler.compile(source)
        # replace handlebars for json profiles
        if self.reference_profiles:
            output_index = template(
                {"profile_from_whylogs": profile_jsons[0], "reference_profile": reference_profile_jsons[0]})
        else:
            output_index = template(
                {"profile_from_whylogs": profile_jsons[0]})
        return self.__display_html(output_index, html_file)

    def download(self, html, path=None):
        # code to write html arg to file and generate name using TimeStamp
        if path:
            output_path = os.path.abspath(os.path.expanduser(path))
        else:
            output_path = 'html_reports/'

        name_of_file = "test"

        completeName = os.path.join(output_path, name_of_file+".html")

        file1 = open(completeName, "w")

        file1.write(html.data)

        file1.close()

    def feature(self, name):
        print(self.profiles[0].dataset_timestamp)
        """
        create profile html
        """
        try:
            from pybars import Compiler
        except ImportError as e:
            Compiler = None
            logger.debug(str(e))
            logger.debug(
                "Unable to load pybars; install pybars3 to load profile from directly from the current session ")
        # create json output from profiles
        if self.profiles:
            if len(self.profiles) > 1:
                logger.warning(
                    "More than one profile not implemented yet, default to first profile in the list ")
            profile_jsons = [message_to_json(each_prof.to_summary())
                             for each_prof in self.profiles]

            if self.reference_profiles:
                reference_profile_jsons = [message_to_json(each_prof.to_summary())
                                           for each_prof in self.reference_profiles]

        else:
            logger.warning(
                "Got no profile data, make sure you pass data correctly ")
            return None
        index_path = os.path.abspath(os.path.join(
            _MY_DIR, os.pardir, "viewer", "index-hbs-cdn-all-in-jupyter-distribution-chart.html"))

        html_file = self.__iframe_output_sizing(
            "index-hbs-cdn-all-in-jupyter-distribution-chart.html")
        with open(index_path, "r") as file_with_template:
            source = file_with_template.read()
            # compile templated files
        compiler = Compiler()
        template = compiler.compile(source)
        # replace handlebars for json profiles
        if self.reference_profiles:
            profile_feature = json.loads(profile_jsons[0])
            reference_profile_feature = json.loads(reference_profile_jsons[0])
            output_index = template(
                {"profile_from_whylogs": json.dumps(profile_feature.get('columns').get(name)), "reference_profile": json.dumps(reference_profile_feature.get('columns').get(name))})
        return self.__display_html(output_index, html_file)
