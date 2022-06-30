import logging
import os
from tempfile import mkdtemp
from typing import Optional, Union

import mlflow

from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.core import DatasetProfileView
from whylogs.core.utils import deprecated_alias
from whylogs.viz.extensions.reports.html_report import HTMLReport

logger = logging.getLogger(__name__)


class MlflowWriter(Writer):
    def __init__(self) -> None:
        self._profile_dir = "whylogs"
        self._profile_name = "whylogs_profile"
        self._end_run = True

    @deprecated_alias(profile="file")
    def write(
        self,
        file: Optional[Union[Writable, DatasetProfileView]] = None,
        dest: Optional[str] = None,
        **kwargs,
    ) -> None:
        run = mlflow.active_run() or mlflow.start_run()
        self._run_id = run.info.run_id
        if isinstance(file, HTMLReport) and dest is None:
            dest = "html_reports/ProfileViz.html"
        output = self._get_temp_directory(run_id=self._run_id, dest=dest)
        file.write(path=output)  # type: ignore
        mlflow.log_artifact(output, artifact_path=self._profile_dir)

        if self._end_run:
            mlflow.end_run()

    def option(
        self, profile_name: Optional[str] = None, profile_dir: Optional[str] = None, end_run: Optional[bool] = None
    ) -> None:
        if end_run:
            self._end_run = end_run
        if profile_dir:
            self._profile_dir = profile_dir
        if profile_name:
            self._profile_name = profile_name

    def _get_temp_directory(self, run_id: str, dest: Optional[str] = None):
        tmp_dir = mkdtemp()
        output_dir = os.path.join(tmp_dir, self._profile_dir)
        os.makedirs(output_dir, exist_ok=True)
        if dest is None:
            output = os.path.join(output_dir, f"{self._profile_name}_{run_id}.bin")
        else:
            output = os.path.join(output_dir, dest)
        return output
