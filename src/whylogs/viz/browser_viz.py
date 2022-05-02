import logging
import os
import tempfile
import webbrowser
from platform import uname
from typing import List

from whylogs.core import DatasetProfile
from whylogs.util.protobuf import message_to_json

_MY_DIR = os.path.realpath(os.path.dirname(__file__))

logger = logging.getLogger(__name__)


def is_wsl():
    return "microsoft-standard-WSL" in uname().release


def profile_viewer(profiles: List[DatasetProfile] = None, reference_profiles: List[DatasetProfile] = None, output_path=None) -> str:
    """
    open a profile viewer loader on your default browser
    """
    try:
        from pybars import Compiler
    except ImportError as e:
        Compiler = None
        logger.debug(str(e))
        logger.warning("Unable to load pybars; install pybars3 to load profile directly from the current session ")
        index_path = os.path.abspath(os.path.join(_MY_DIR, os.pardir, "viewer/templates", "index.html"))
        webbrowser.open_new_tab(f"file:{index_path}#")
        return None

    # create json output from profiles
    if profiles:
        if len(profiles) > 1:
            logger.warning("More than one profile not implemented yet, default to first profile in the list ")
        profile_jsons = [message_to_json(each_prof.to_summary()) for each_prof in profiles]
        if reference_profiles:
            reference_profile_jsons = [message_to_json(each_prof.to_summary()) for each_prof in reference_profiles]

    else:
        index_path = os.path.abspath(os.path.join(_MY_DIR, os.pardir, "viewer/templates", "index.html"))
        webbrowser.open_new_tab(f"file:{index_path}#")
        return None

    index_path = os.path.abspath(os.path.join(_MY_DIR, os.pardir, "viewer/templates", "index-hbs-cdn-all-in.html"))

    with open(index_path, "r") as file_with_template:
        source = file_with_template.read()

    # compile templated files
    compiler = Compiler()
    template = compiler.compile(source)
    # replace handlebars for json profiles
    if reference_profiles:
        output_index = template({"profile_from_whylogs": profile_jsons[0], "reference_profile": reference_profile_jsons[0]})
    else:
        output_index = template({"profile_from_whylogs": profile_jsons[0]})

    if not output_path:
        output_path = tempfile.mkstemp(suffix=".html")[1]
    else:
        output_path = os.path.abspath(os.path.expanduser(output_path))
    with open(output_path, "w") as f:
        f.write(output_index)

    # fix relative filepath for browser if executing within WSL
    browser_file = f"file://wsl%24/Ubuntu{output_path}#" if is_wsl() else f"file:{output_path}#"

    webbrowser.open_new_tab(browser_file)
    return output_path
