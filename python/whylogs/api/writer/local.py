import logging
import os
from typing import Optional

from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.core import DatasetProfileView

logger = logging.getLogger(__name__)


class LocalWriter(Writer):
    def __init__(self, base_dir: Optional[str] = None, base_name: Optional[str] = None) -> None:
        if base_dir is None:
            base_dir = os.getcwd()
        if base_name is None:
            base_name = "profile"

        self._base_dir = base_dir
        self._base_name = base_name

    def write(
        self,
        file: Optional[Writable] = None,
        profile: Optional[DatasetProfileView] = None,
        dest: Optional[str] = None,
        **kwargs,
    ) -> None:
        if profile:
            logger.warning("You should use `file` instead", DeprecationWarning)
            file = profile
        if isinstance(file, DatasetProfileView) and dest is None:
            dest = f"{self._base_name}_{profile.creation_timestamp}.bin"
        elif dest is None:
            dest = "html_reports/ProfileViz.html"
        full_path = os.path.join(self._base_dir, dest)
        file.write(full_path)  # type: ignore

    def option(self, base_dir: Optional[str] = None, base_name: Optional[str] = None) -> "LocalWriter":  # type: ignore
        if base_dir is not None:
            self._base_dir = base_dir
        if base_name is not None:
            self._base_name = base_name
        return self
