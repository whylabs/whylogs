import logging
import os
from tempfile import mkdtemp
from typing import Any, List, Optional, Tuple, Union

import mlflow

from whylogs.api.usage_stats import emit_usage
from whylogs.api.writer import Writer
from whylogs.api.writer.writer import _Writable
from whylogs.core.utils import deprecated_alias

logger = logging.getLogger(__name__)


class MlflowWriter(Writer):
    def __init__(self) -> None:
        self._file_dir = "whylogs"
        self._file_name = None
        self._end_run = True
        emit_usage("mlflow_writer")

    @deprecated_alias(profile="file")
    def write(
        self,
        file: _Writable,
        dest: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bool, Union[str, List[Tuple[bool, str]]]]:
        preexisting_run = mlflow.active_run()
        run = preexisting_run or mlflow.start_run()
        self._run_id = run.info.run_id
        dest = dest or self._file_name  # dest has a higher priority than file_name
        output = self._get_temp_directory(dest=dest)
        success, files = file._write(path=output, filename=self._file_name)
        if not success:
            return False, "Failed to write temporary file(s)"

        files = files if isinstance(files, list) else [files]
        for file in files:
            mlflow.log_artifact(file, artifact_path=self._file_dir)

        if self._end_run is True and not preexisting_run:
            mlflow.end_run()
        return True, f"MLFlowed {files} -> {self._file_dir}"

    def option(self, **kwargs: Any) -> Writer:
        """
        file_name: str   Filename to write to
        file_dir: str    Directory to write to
        end_run: bool    End MLFlow run (only if it was created by the write() call)
        """
        file_name = kwargs.get("file_name")
        file_dir = kwargs.get("file_dir")
        end_run = kwargs.get("end_run")
        if end_run is not None:
            self._end_run = end_run
        if file_dir:
            self._file_dir = file_dir
        if file_name:
            self._file_name = file_name  # type: ignore
        return self

    def _get_temp_directory(self, dest: Optional[str] = None):
        tmp_dir = mkdtemp()
        output_dir = os.path.join(tmp_dir, self._file_dir)
        os.makedirs(output_dir, exist_ok=True)
        if dest:
            output = os.path.join(output_dir, dest)
        else:
            output = output_dir
        return output
