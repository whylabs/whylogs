import os
import shutil

import mlflow
import pytest

import whylogs as why
from whylogs.api.logger.result_set import ViewResultSet
from whylogs.api.writer.mlflow import MlflowWriter


class TestMlflowWriter(object):
    @classmethod
    def teardown_class(cls):
        shutil.rmtree("mlruns")
        shutil.rmtree("artifact_downloads")

    @pytest.fixture
    def mlflow_writer(self):
        writer = MlflowWriter()
        return writer

    def test_writes_profile_to_mlflow_experiment(self, profile_view, mlflow_writer):
        mlflow_writer.write(profile_view)
        run_id = mlflow_writer._run_id
        file_dir = f"artifacts/{mlflow_writer._profile_dir}"
        file_path = os.path.join(file_dir, f"{mlflow_writer._profile_name}_{run_id}.bin")
        assert os.path.isfile(f"mlruns/0/{run_id}/{file_path}")

    def test_get_temp_directory(self, mlflow_writer):
        run_id = "example_run_id"

        # default path
        actual_url = mlflow_writer._get_temp_directory(run_id)
        expected_url = "whylogs/whylogs_profile_" + run_id + ".bin"
        assert expected_url in actual_url

        # modified with option
        mlflow_writer.option(profile_dir="other", profile_name="profile_name")
        actual_modified_url = mlflow_writer._get_temp_directory(run_id)
        expected_modified_url = "other/profile_name_" + run_id + ".bin"
        assert expected_modified_url in actual_modified_url

    def test_result_set_api_creates_file_in_mlflow(self, result_set):
        run = mlflow.start_run()
        run_id = run.info.run_id

        result_set.writer("mlflow").write()

        # verify we can fetch the profile with mlflow
        client = mlflow.tracking.MlflowClient()

        local_dir = "artifact_downloads"
        if not os.path.exists(local_dir):
            os.mkdir(local_dir)
        local_path = client.download_artifacts(run_id, "whylogs", local_dir)
        read_profile = why.read(path=f"{local_path}/whylogs_profile_{run_id}.bin")

        assert isinstance(read_profile, ViewResultSet)
