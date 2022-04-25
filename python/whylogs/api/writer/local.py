import os
from typing import Optional

from whylogs.api.writer import Writer
from whylogs.core import DatasetProfileView


class LocalWriter(Writer):
    def __init__(self) -> None:
        self._base_dir: str = os.getcwd()
        self._filename_format = "profiles_{dataset_timestamp}_{creation_timestamp}.bin"

    def write(self, profile: DatasetProfileView, dest: Optional[str] = None) -> None:
        if dest is None:
            dest = self._filename_format.format(dataset_timestamp=0, creation_timestamp=0)
        full_path = os.path.join(self._base_dir, dest)
        profile.write(full_path)

    def option(  # type: ignore
        self, base_dir: Optional[str] = None, filename_format: Optional[str] = None
    ) -> "LocalWriter":
        if base_dir is not None:
            self._base_dir = base_dir
        elif filename_format is not None:
            self._filename_format = filename_format

        return self
