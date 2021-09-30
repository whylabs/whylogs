import os
from unittest import mock
from unittest.mock import patch


@patch("whylogs.mlflow.patcher._is_patched", False)
def test_import_error():
    with mock.patch.dict("sys.modules", {"mlflow": None}):
        import whylogs

        assert not whylogs.enable_mlflow()


def test_mlflow_patched(mlflow_config_path):
    import mlflow

    import whylogs
    from whylogs.app.config import load_config
    from whylogs.app.session import session_from_config

    assert os.path.exists(mlflow_config_path)

    config = load_config(mlflow_config_path)
    session = session_from_config(config)

    assert whylogs.enable_mlflow(session)
    assert mlflow.whylogs is not None
    print("HEY LISTEN")
    whylogs.mlflow.disable_mlflow()


def test_patch_multiple_times(mlflow_config_path):
    import whylogs
    from whylogs.app.config import load_config
    from whylogs.app.session import session_from_config

    assert os.path.exists(mlflow_config_path)

    config = load_config(mlflow_config_path)
    session = session_from_config(config)

    # patch three times
    assert whylogs.enable_mlflow(session)
    assert whylogs.enable_mlflow(session)
    assert whylogs.enable_mlflow(session)

    import mlflow

    assert mlflow.whylogs is not None
    whylogs.mlflow.disable_mlflow()


def test_assert_whylogsrun_close_is_called(tmpdir, mlflow_config_path):
    import mlflow

    import whylogs
    from whylogs.app.config import load_config
    from whylogs.app.session import session_from_config

    assert os.path.exists(mlflow_config_path)

    config = load_config(mlflow_config_path)
    session = session_from_config(config)

    set_up_mlflow(mlflow, tmpdir)
    with mock.patch.object(whylogs.mlflow.patcher.WhyLogsRun, "_close") as mock_close:
        whylogs.enable_mlflow(session)
        with mlflow.start_run():
            pass

        mock_close.assert_called_once()
    whylogs.mlflow.disable_mlflow()


def set_up_mlflow(mlflow, tmpdir):
    mlflow.set_tracking_uri(f"file:{str(tmpdir)}")
    mlflow.create_experiment("default")


def test_assert_log_artifact_is_called(tmpdir, mlflow_config_path):
    import mlflow

    import whylogs
    from whylogs.app.config import load_config
    from whylogs.app.session import session_from_config

    assert os.path.exists(mlflow_config_path)

    config = load_config(mlflow_config_path)
    session = session_from_config(config)

    set_up_mlflow(mlflow, tmpdir)
    with mock.patch.object(mlflow, "log_artifact") as log_artifact:
        whylogs.enable_mlflow(session)
        with mlflow.start_run():
            mlflow.whylogs.log(features={"a": 1})

        log_artifact.assert_called_once()

    whylogs.mlflow.disable_mlflow()


def test_assert_log_artifact_is_called_twice(tmpdir, mlflow_config_path):
    import mlflow

    import whylogs
    from whylogs.app.config import load_config
    from whylogs.app.session import session_from_config

    assert os.path.exists(mlflow_config_path)

    config = load_config(mlflow_config_path)
    session = session_from_config(config)

    set_up_mlflow(mlflow, tmpdir)
    with mock.patch.object(mlflow, "log_artifact") as log_artifact:
        whylogs.enable_mlflow(session)

        with mlflow.start_run():
            mlflow.whylogs.log(features={"a": 1})
            mlflow.whylogs.log(dataset_name="another", features={"a": 1})

        assert log_artifact.call_count == 1
    whylogs.mlflow.disable_mlflow()


def test_sklearn_model_log(tmpdir):
    import mlflow
    from sklearn.linear_model import ElasticNet

    import whylogs
    from whylogs.mlflow import patcher as whylogs_patcher

    set_up_mlflow(mlflow, tmpdir)
    with mock.patch.object(whylogs_patcher, "new_model_log") as new_log_func:
        whylogs.enable_mlflow()

        with mlflow.start_run():
            model = ElasticNet(alpha=0.5, l1_ratio=0.5, random_state=42)
            mlflow.sklearn.log_model(model, "model", registered_model_name="TestModel")

        assert new_log_func.call_count == 1
    whylogs.mlflow.disable_mlflow()
