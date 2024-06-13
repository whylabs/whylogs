import logging
import os
from typing import Any, List, Optional, Tuple, Union

from whylogs.api.writer import Writer
from whylogs.api.writer.writer import _Writable
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
        file: _Writable,
        dest: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bool, Union[str, List[Tuple[bool, str]]]]:
        """
        kwargs:
           base_dir: str    Directory to write to
           base_name: str   File name to write to
           zip: bool       Create a ZipFile containing the Writable's file(s) if the Writer supports it
           zip_name: str   Name of the ZipFile, will be auto-generated if None
        """
        self.option(**kwargs)
        filename = dest or self._base_name
        success, files = file._write(self._base_dir, filename, **kwargs)
        if not success:
            return False, "_Writable failed to create temporary file(s)"

        zipit = kwargs.get("zip")
        if zipit and success:
            zip_name = self._create_zip(files, dest or self._base_dir, kwargs.get("zip_name"))
            return True, [(True, f"Wrote {zip_name}")]

        return success, (files if isinstance(files, str) else zip([success] * len(files), files))  # type: ignore

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
