import logging
import os
from typing import Any, List, Optional, Tuple, Union

from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.core.utils import deprecated_alias

logger = logging.getLogger(__name__)


class LocalWriter(Writer):
    """
    Writes Writable to local files. By default, this just returns the files created in the
    current working directory by the Writable itself. This can be overriden by specifying
    `base_dir` or `base_name` via the constructor or option().

    The Writable optionally can be written to a ZipFile.
    """

    def __init__(self, base_dir: Optional[str] = None, base_name: Optional[str] = None) -> None:
        self._base_dir = base_dir or os.getcwd()
        self._base_name = base_name

    @deprecated_alias(profile="file")
    def write(
        self,
        file: Writable,
        dest: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bool, Union[str, List[Tuple[bool, str]]]]:
        """
        kwargs:
           zip: bool       Create a ZipFile containing the Writable's file(s) if the Writer supports it
           zip_name: str   Name of the ZipFile, will be auto-generated if None
        """
        success, files = file.write(dest or self._base_dir, self._base_name, **kwargs)
        if not success:
            return False, "Writable failed to create temporary file(s)"

        zipit = kwargs.get("zip")
        if zipit and success:
            zip_name = self._create_zip(files, dest or self._base_dir, kwargs.get("zip_name"))
            return True, [(True, f"Wrote {zip_name}")]

        files = [files] if isinstance(files, str) else files
        return success, f"Wrote {', '.join(files)}"

    def option(self, **kwargs: Any) -> Writer:
        """
        base_dir: str    Directory to write to
        base_name: str   File name to write to
        """
        base_dir = kwargs.get("base_dir")
        base_name = kwargs.get("base_name")
        if base_dir is not None:
            self._base_dir = base_dir
        if base_name is not None:
            self._base_name = base_name
        return self
