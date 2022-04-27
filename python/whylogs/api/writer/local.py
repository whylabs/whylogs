import os
from typing import Optional

from whylogs.api.writer import Writer
from whylogs.core import DatasetProfileView


class LocalWriter(Writer):
    def __init__(self, base_dir: Optional[str] = None, base_name: Optional[str] = None) -> None:
        if base_dir is None:
            base_dir = os.getcwd()
        if base_name is None:
            base_name = "profile"

        self._base_dir = base_dir
        self._base_name = base_name

    def write(self, profile: DatasetProfileView, dest: Optional[str] = None) -> None:
        if dest is None:
            dest = f"{self._base_name}_{profile.creation_timestamp}.bin"
        full_path = os.path.join(self._base_dir, dest)
        profile.write(full_path)

    def option(self, base_dir: Optional[str] = None, base_name: Optional[str] = None) -> "LocalWriter":  # type: ignore
        if base_dir is not None:
            self._base_dir = base_dir
        if base_name is not None:
            self._base_name = base_name

        return self
