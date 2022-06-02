import os
from tempfile import mkdtemp
from typing import Optional

import mlflow

from whylogs.api.writer import Writer
from whylogs.core import DatasetProfileView


class MlflowWriter(Writer):
    def __init__(self) -> None:
        self._profile_dir = "whylogs"
        self._profile_name = "whylogs_profile"
        self._end_run = True

    def write(self, profile: DatasetProfileView) -> None:
        run = mlflow.active_run() or mlflow.start_run()
        self._run_id = run.info.run_id

        output = self._get_temp_directory(run_id=self._run_id)
        profile.write(path=output)
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

    def _get_temp_directory(self, run_id):
        tmp_dir = mkdtemp()
        output_dir = os.path.join(tmp_dir, self._profile_dir)
        os.makedirs(output_dir, exist_ok=True)
        output = os.path.join(output_dir, f"{self._profile_name}_{run_id}.bin")
        return output
