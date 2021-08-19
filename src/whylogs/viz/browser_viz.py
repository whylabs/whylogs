import logging
import os
import tempfile
import webbrowser
from typing import List

from whylogs.core import DatasetProfile
from whylogs.util.protobuf import message_to_json

_MY_DIR = os.path.realpath(os.path.dirname(__file__))

logger = logging.getLogger(__name__)


def profile_viewer(profiles: List[DatasetProfile] = None, output_path=None) -> str:
    """
    open a profile viewer loader on your default browser
    """
    try:
        from pybars import Compiler
    except ImportError as e:
        Compiler = None
        logger.debug(str(e))
        logger.debug("Unable to load pybars; install pybars3 to load profile from directly from the current session ")

        index_path = os.path.abspath(os.path.join(_MY_DIR, os.pardir, "viewer", "index.html"))
        webbrowser.open_new_tab(f"file:{index_path}#")
        return None

    # create json output from profiles
    if profiles:
        if len(profiles) > 1:
            logger.warning("More than one profile not implemented yet, default to first profile in the list ")
        profile_jsons = [message_to_json(each_prof.to_summary()) for each_prof in profiles]
    else:
        index_path = os.path.abspath(os.path.join(_MY_DIR, os.pardir, "viewer", "index.html"))
        webbrowser.open_new_tab(f"file:{index_path}#")
        return None

    index_path = os.path.abspath(os.path.join(_MY_DIR, os.pardir, "viewer", "index-hbs-cdn-all-in.html"))

    with open(index_path, "r") as file_with_template:
        source = file_with_template.read()

    # compile templated files
    compiler = Compiler()
    template = compiler.compile(source)
    # replace handlebars for json profiles
    output_index = template({"profile_from_whylogs": profile_jsons[0]})

    if not output_path:
        output_path = tempfile.mkstemp(suffix=".html")[1]
    else:
        output_path = os.path.abspath(os.path.expanduser(output_path))
    with open(output_path, "w") as f:
        f.write(output_index)

    webbrowser.open_new_tab(f"file:{output_path}#")
    return output_path
