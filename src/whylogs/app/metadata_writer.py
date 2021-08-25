import json
import os
from glob import glob
from logging import getLogger
from string import Template
from typing import Dict, List, Optional, Union

from smart_open import open

from .config import MetadataConfig

DEFAULT_PATH_TEMPLATE = "$name/metadata"

logger = getLogger(__name__)


class MetadataWriter:
    """
    Class for writing metadata to disk

    Parameters
    ----------
    output_path : str
        Prefix of where to output files.  A directory for `type = 'local'`,
        or key prefix for `type = 's3'`
    path_template : str, optional
        Templatized path output using standard python string templates.
        Variables are accessed via $identifier or ${identifier}.
        See :func:`MetadataWriter.template_params` for a list of available
        identifers.
        Default = :data:`DEFAULT_PATH_TEMPLATE`
    """

    def __init__(
        self,
        output_path: str,
        input_path: Optional[str] = "",
        path_template: Optional[str] = None,
        writer_type: Optional[str] = "local",
    ):
        self.output_path = output_path
        self.input_path = input_path
        self.writer_type = writer_type
        if path_template is None:
            path_template = DEFAULT_PATH_TEMPLATE
        self.path_template = Template(path_template)

    def path_suffix(self, name) -> str:
        """
        Generate a path string for an output path from the given arguments by
        applying the path templating defined in `self.path_template`
        """
        kwargs = {"name": name}
        return self.path_template.substitute(**kwargs)

    def autosegmentation_write(self, name: str, segments: Union[List[Dict], List[str]]) -> None:
        path = os.path.join(self.output_path, self.path_suffix(name))
        if self.writer_type == "local":
            os.makedirs(path, exist_ok=True)
        output_file = os.path.join(path, "segments.json")

        with open(output_file, "wt") as f:
            f.write(json.dumps(segments))

    def autosegmentation_read(self):
        valid_paths = []
        test_paths = [""]
        test_paths.extend(self.input_path.split(":"))
        test_paths.append(self.output_path)

        for path in test_paths:
            valid_paths.extend(glob(os.path.join(path, "segments.json")))
            valid_paths.extend(glob(os.path.join(path, self.path_suffix("*"), "segments.json")))

        if len(valid_paths) <= 0:
            logger.error(
                "No autosegmentation settings found. Update the session "
                "configuration settings or run `session.estimate_segments`."
                "\nLogging full profile with no segments."
            )
            return None
        elif len(valid_paths) > 1:
            logger.warning(
                "Multiple autosegmentation settings found. Choosing first "
                "valid path. To choose a different file, pass unique "
                "substring during logging via parameter "
                "segments='auto:<search>'."
            )
            logger.info(f"\nAutosegmentation paths found:\n{valid_paths}")

        try:
            f = open(valid_paths[0], "rt")
            segments = json.load(f)
            logger.info(f"Segmenting using path: {valid_paths[0]}")
        except ValueError:
            segments = []

        return segments


def metadata_from_config(config: MetadataConfig):
    """
    Construct a whylogs `MetadataWriter` from a `MetadataConfig`

    Returns
    -------
    metadata_writer: MetadataWriter
        whylogs metadata writer
    """
    if config.type == "local":
        abs_path = os.path.abspath(config.output_path)
        if not os.path.exists(abs_path):
            os.makedirs(abs_path, exist_ok=True)

    return MetadataWriter(config.output_path, config.input_path, config.path_template, config.type)
