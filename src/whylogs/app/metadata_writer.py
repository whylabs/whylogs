import json
import os
from string import Template
from typing import Dict, List, Optional, Union

from .config import MetadataConfig

DEFAULT_PATH_TEMPLATE = "$name/metadata"


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
        path_template: Optional[str] = None,
        writer_type: Optional[str] = "local",
    ):
        self.output_path = output_path
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

    def autosegmentation_write(
            self,
            name: str,
            segments: Union[List[Dict], List[str]]) -> None:
        path = os.path.join(self.output_path, self.path_suffix(name))
        if self.writer_type == "local":
            os.makedirs(path, exist_ok=True)
        output_file = os.path.join(path, "segments.json")

        with open(output_file, "wt") as f:
            f.write(json.dumps(segments))


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

    return MetadataWriter(
            config.output_path,
            config.path_template,
            config.type
    )
