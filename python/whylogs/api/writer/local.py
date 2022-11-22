import logging
import os
from typing import Any, Optional, Tuple

from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.core.utils import deprecated_alias

logger = logging.getLogger(__name__)


class LocalWriter(Writer):
    def __init__(self, base_dir: Optional[str] = None, base_name: Optional[str] = None) -> None:
        if base_dir is None:
            base_dir = os.getcwd()
        self._base_dir = base_dir
        self._base_name = base_name

    @deprecated_alias(profile="file")
    def write(
        self,
        file: Writable,
        dest: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bool, str]:
        dest = dest or self._base_name or file.get_default_path()  # type: ignore
        full_path = os.path.join(self._base_dir, dest)
        file.write(full_path, **kwargs)
        return True, full_path

    def option(self, base_dir: Optional[str] = None, base_name: Optional[str] = None) -> "LocalWriter":  # type: ignore
        if base_dir is not None:
            self._base_dir = base_dir
        if base_name is not None:
            self._base_name = base_name
        return self
