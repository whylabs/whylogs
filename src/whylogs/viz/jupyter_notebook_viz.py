import logging
import os
import tempfile
import webbrowser
from typing import List
import html
import IPython

from whylogs.core import DatasetProfile
from whylogs.util.protobuf import message_to_json


_MY_DIR = os.path.realpath(os.path.dirname(__file__))

logger = logging.getLogger(__name__)


def display_profile(profiles: List[DatasetProfile] = None, reference_profiles: List[DatasetProfile] = None) -> str:
    """
    display profile in jupyter notebook
    """
    try:
        from pybars import Compiler
    except ImportError as e:
        Compiler = None
        logger.debug(str(e))
        logger.debug(
            "Unable to load pybars; install pybars3 to load profile from directly from the current session ")

    # create json output from profiles
    if profiles:
        if len(profiles) > 1:
            logger.warning(
                "More than one profile not implemented yet, default to first profile in the list ")
        profile_jsons = [message_to_json(each_prof.to_summary()) for each_prof in profiles]
        if reference_profiles:
            reference_profile_jsons = [message_to_json(each_prof.to_summary())
                                       for each_prof in reference_profiles]

    else:
        logger.warning(
            "Got no profile data, make sure you pass data correctly ")
        return None

    index_path = os.path.abspath(os.path.join(
        _MY_DIR, os.pardir, "viewer", "index-hbs-cdn-all-in-for-jupyter-notebook.html"))

    with open(index_path, "r") as file_with_template:
        source = file_with_template.read()

    # compile templated files
    compiler = Compiler()
    template = compiler.compile(source)
    # replace handlebars for json profiles
    if reference_profiles:
        output_index = template(
            {"profile_from_whylogs": profile_jsons[0], "reference_profile": reference_profile_jsons[0]})
    else:
        output_index = template(
            {"profile_from_whylogs": profile_jsons[0]})

    iframe = f'''<iframe srcdoc="{html.escape(output_index)}" width=960px height=1000px frameBorder=0></iframe>'''
    return IPython.display.HTML(iframe)
