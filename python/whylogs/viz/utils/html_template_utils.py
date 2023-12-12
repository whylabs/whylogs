import logging
import os
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)
_file_dir = Path(os.path.dirname(__file__))
_VIZ_DIR = _file_dir.parent.absolute()


def _get_template_path(html_file_name: str) -> str:
    template_path = os.path.abspath(os.path.join(_VIZ_DIR, "html", "templates", html_file_name))
    return template_path


def _get_compiled_template(template_name: str) -> "Callable":
    template_path = _get_template_path(template_name)
    try:
        from pybars import Compiler  # type: ignore
    except ImportError as e:
        msg = "Unable to load pybars; install pybars3 to load profile directly from the current session or \
        pip install whylogs[viz] to install dependencies for this module"
        logger.debug(e, exc_info=True)
        logger.error(msg)
        raise ImportError(msg)

    with open(template_path, "rt") as file_with_template:
        source = file_with_template.read()
    return Compiler().compile(source)


def get_compiled_template(template_name: str) -> "Callable":
    return _get_compiled_template(template_name=template_name)
