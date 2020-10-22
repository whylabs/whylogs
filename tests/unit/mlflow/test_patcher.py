from importlib import reload
from unittest import mock


def test_import_error():
    with mock.patch.dict("sys.modules", {"mlflow": None}):
        import whylogs

        assert not whylogs.enable_mlflow()


def test_mlflow_patched():
    import whylogs
    import mlflow

    assert whylogs.enable_mlflow()

    assert mlflow.whylogs is not None
    whylogs.mlflow.disable_mlflow()


def test_patch_multiple_times():
    import whylogs

    # patch three times
    assert whylogs.enable_mlflow()
    assert whylogs.enable_mlflow()
    assert whylogs.enable_mlflow()

    import mlflow

    assert mlflow.whylogs is not None
    whylogs.mlflow.disable_mlflow()


def test_assert_whylogsrun_close_is_called(tmpdir):
    import whylogs
    import mlflow

    set_up_mlflow(mlflow, tmpdir)
    with mock.patch.object(whylogs.mlflow.patcher.WhyLogsRun, "_close") as mock_close:
        whylogs.enable_mlflow()
        with mlflow.start_run():
            pass

        mock_close.assert_called_once()
    whylogs.mlflow.disable_mlflow()


def set_up_mlflow(mlflow, tmpdir):
    mlflow.set_tracking_uri(f"file:{str(tmpdir)}")
    mlflow.create_experiment("default")


def test_assert_log_artifact_is_called(tmpdir):
    import whylogs
    import mlflow

    set_up_mlflow(mlflow, tmpdir)
    with mock.patch.object(mlflow, "log_artifact") as log_artifact:
        whylogs.enable_mlflow()
        with mlflow.start_run():
            mlflow.whylogs.log(features={"a": 1})

        log_artifact.assert_called_once()

    whylogs.mlflow.disable_mlflow()


def test_assert_log_artifact_is_called_twice(tmpdir):
    import whylogs
    import mlflow

    set_up_mlflow(mlflow, tmpdir)
    with mock.patch.object(mlflow, "log_artifact") as log_artifact:
        whylogs.enable_mlflow()

        with mlflow.start_run():
            mlflow.whylogs.log(features={"a": 1})
            mlflow.whylogs.log(dataset_name="another", features={"a": 1})

        assert log_artifact.call_count == 2
    whylogs.mlflow.disable_mlflow()
