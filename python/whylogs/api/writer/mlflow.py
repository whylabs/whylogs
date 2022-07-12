import logging
import os
from tempfile import mkdtemp
from typing import Any, Optional

import mlflow

from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.core.utils import deprecated_alias

logger = logging.getLogger(__name__)


class MlflowWriter(Writer):
    def __init__(self) -> None:
        self._file_dir = "whylogs"
        self._file_name = None
        self._end_run = True

    @deprecated_alias(profile="file")
    def write(
        self,
        file: Writable,
        dest: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        run = mlflow.active_run() or mlflow.start_run()
        self._run_id = run.info.run_id
        dest = dest or self._file_name or file.get_default_path()  # dest has a higher priority than file_name
        output = self._get_temp_directory(dest=dest)
        file.write(path=output)  # type: ignore
        mlflow.log_artifact(output, artifact_path=self._file_dir)

        if self._end_run is True:
            mlflow.end_run()

    @deprecated_alias(profile_dir="file_dir", profile_name="file_name")
    def option(
        self, file_name: Optional[str] = None, file_dir: Optional[str] = None, end_run: Optional[bool] = None
    ) -> None:
        if end_run is not None:
            self._end_run = end_run
        if file_dir:
            self._file_dir = file_dir
        if file_name:
            self._file_name = file_name  # type: ignore

    def _get_temp_directory(self, dest: Optional[str] = None):
        tmp_dir = mkdtemp()
        output_dir = os.path.join(tmp_dir, self._file_dir)
        os.makedirs(output_dir, exist_ok=True)
        if dest:
            output = os.path.join(output_dir, dest)
        elif self._file_name:
            output = os.path.join(output_dir, self._file_name)
        else:
            output = output_dir
        return output
