import os
import tempfile
from abc import ABC, abstractmethod
from typing import IO, Any, List, Optional, Tuple, Union
from zipfile import ZipFile

# from whylogs.core.utils.utils import deprecated


class _Writable(ABC):
    """
    A Writable is an object that contains data to write to a file or files.
    These might be temporary files intended to be passed on to another
    consumer (e.g., WhyLabs servers) via a Writer.
    """

    @staticmethod
    def _safe_open_write(path: str, mode: str = "t") -> IO[Any]:
        """
        Open `path` for writing (including filename), creating any parent directories as needed.
        `mode` is appended to 'w' in the mode argument to open().
        """
        try:
            if os.path.dirname(path):
                os.makedirs(os.path.dirname(path), exist_ok=True)
        except FileExistsError:
            pass
        return open(path, f"w{mode}")

    @abstractmethod
    def _get_default_filename(self) -> str:
        """Returns filename to write if None passed to Writable::write()"""

    def _get_default_path(self) -> Optional[str]:
        """
        Returns the default path to write to, excluding filename(s). None = use CWD
        """
        return os.getcwd()

    def write(self, path: Optional[str] = None, **kwargs: Any) -> Tuple[bool, str]:
        success, files = self._write("", path, **kwargs)
        files = files[0] if isinstance(files, list) else files
        return success, files

    @abstractmethod
    def _write(
        self, path: Optional[str] = None, filename: Optional[str] = None, **kwargs: Any
    ) -> Tuple[bool, Union[str, List[str]]]:  # TODO: Union[str, List[Tupble[bool, str]]] ?
        """
        Writes the Writable's data to file(s) under `path` (excluding filename(s)) if specified,
        else under `_get_default_path()`. `filename` specifies the file to write there. Writables
        can generate a default filename if None is specified. Returns a bool that's True iff all
        files were written successfully and a list of the filenames (including path) written.
        """
        return False, list()

    def writer(self, name: str = "local", **kwargs: Any) -> "WriterWrapper":
        "Utility method to create a Writer of the specified type"
        return Writers.get(name, self, **kwargs)  # type: ignore


Writable = _Writable


class Writer(ABC):
    # noinspection PyMethodMayBeStatic
    def check_interval(self, interval_seconds: int) -> None:
        """
        Validate an interval configuration for a given writer.

        Some writers only accepts certain interval configuration. Raise BadConfigError for
        an unacceptable interval.
        """

    def _create_zip(self, files: List[str], dir: Optional[str] = None, zip_name: Optional[str] = None) -> str:
        with tempfile.NamedTemporaryFile(suffix=".zip", prefix=zip_name, dir=dir, delete=False) as tmp_file:
            with ZipFile(tmp_file, "w", allowZip64=True) as zip_file:
                for file in files:
                    zip_file.write(file, file.split(os.sep)[-1])
                    # TODO: delete file?

            tmp_file.flush()
            tmp_file.seek(0)
            return tmp_file.name

    @abstractmethod
    def write(
        self,
        file: _Writable,
        dest: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bool, Union[str, List[Tuple[bool, str]]]]:
        """
        Calls Writable::write() to serialize the Wriable `file` to temporary file(s),
        then "sends" the temporary file(s) to `dest` according to the type of Writer.
        If `dest` is None, it uses the Writer's default destination.

        kwargs:
            zip: bool       Create a ZipFile containing the Writable's file(s) if the Writer supports it
            zip_name: str   Name of the ZipFile, will be auto-generated if None
        """
        return False, "unimplemented abstract method"

    @abstractmethod
    def option(self, **kwargs: Any) -> "Writer":
        return self


class WriterWrapper:
    """Elide the Writable argument"""

    def __init__(self, writable: _Writable, writer: Writer):
        self._writable = writable
        self._writer = writer

    def check_interval(self, interval_seconds: int) -> None:
        self._writer.check_interval(interval_seconds)

    def write(
        self,
        dest: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bool, Union[str, List[Tuple[bool, str]]]]:
        return self._writer.write(self._writable, dest, **kwargs)

    def option(self, **kwargs: Any) -> "WriterWrapper":
        self._writer = self._writer.option(**kwargs)
        return self


class Writers:
    @staticmethod
    def get(name: str, writable: Optional[_Writable] = None, **kwargs) -> Union[Writer, WriterWrapper]:
        if name == "local":
            from whylogs.api.writer.local import LocalWriter

            writer = LocalWriter(**kwargs)  # type: ignore
        elif name == "whylabs":
            from whylogs.api.writer.whylabs import WhyLabsWriter

            writer = WhyLabsWriter(**kwargs)  # type: ignore
        elif name == "s3":
            from whylogs.api.writer.s3 import S3Writer

            writer = S3Writer(**kwargs)  # type: ignore
        elif name == "mlflow":
            from whylogs.api.writer.mlflow import MlflowWriter

            writer = MlflowWriter(**kwargs)  # type: ignore
        elif name == "gcs":
            from whylogs.api.writer.gcs import GCSWriter

            writer = GCSWriter(**kwargs)  # type: ignore
        else:
            raise ValueError(f"Unrecognized writer: {name}")

        if writable is not None:
            return WriterWrapper(writable, writer)
        return writer
