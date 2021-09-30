import os

def set_up_mlflow(mlflow, tmpdir):
    mlflow.set_tracking_uri(f"file:{str(tmpdir)}")
    mlflow.create_experiment("default")


def test_listRuns_shouldReturn_NoRuns(tmpdir, mlflow_config_path):
    import mlflow
    import whylogs
    from whylogs.app.session import session_from_config
    from whylogs.app.config import load_config

    assert os.path.exists(mlflow_config_path)

    config = load_config(mlflow_config_path)
    session = session_from_config(config)

    set_up_mlflow(mlflow, tmpdir)
    whylogs.enable_mlflow(session)

    for i in range(0, 10):
        with mlflow.start_run():
            pass

    assert len(mlflow.list_run_infos("0")) == 10
    assert len(whylogs.mlflow.list_whylogs_runs("0")) == 0
    whylogs.mlflow.disable_mlflow()


def test_listRuns_shouldReturn_CorrectRunCount(tmpdir, mlflow_config_path):
    import mlflow
    import whylogs
    from whylogs.app.session import session_from_config
    from whylogs.app.config import load_config

    assert os.path.exists(mlflow_config_path)

    config = load_config(mlflow_config_path)
    session = session_from_config(config)

    set_up_mlflow(mlflow, tmpdir)
    whylogs.enable_mlflow(session)

    for i in range(0, 10):
        with mlflow.start_run():
            if i % 2 == 0:
                mlflow.whylogs.log(features={"a": 1})
    print("WEIRD")
    assert len(mlflow.list_run_infos("0")) == 10
    assert len(whylogs.mlflow.list_whylogs_runs("0")) == 5
    assert len(whylogs.mlflow.get_experiment_profiles("0")) == 5
    whylogs.mlflow.disable_mlflow()


def test_get_run_profiles_shouldReturn_multipleProfiles(tmpdir, mlflow_config_path):
    import mlflow

    import whylogs
    from whylogs.app.session import session_from_config
    from whylogs.app.config import load_config

    assert os.path.exists(mlflow_config_path)

    config = load_config(mlflow_config_path)
    session = session_from_config(config)

    set_up_mlflow(mlflow, tmpdir)
    whylogs.enable_mlflow(session)

    with mlflow.start_run():
        mlflow.whylogs.log(features={"a": 1})
        mlflow.whylogs.log(features={"a": 1}, dataset_name="another-profile")

    with mlflow.start_run():
        mlflow.whylogs.log(features={"a": 1}, dataset_name="another-profile")

    runs = whylogs.mlflow.list_whylogs_runs("0")
    default_profiles = whylogs.mlflow.get_run_profiles(run_id=runs[0].run_id)
    another_profile = whylogs.mlflow.get_run_profiles(run_id=runs[0].run_id, dataset_name="another-profile")

    assert len(runs) == 2
    # verify the number of profiles for each datasetname
    assert len(whylogs.mlflow.get_experiment_profiles("0", dataset_name="default")) == 2
    assert len(whylogs.mlflow.get_experiment_profiles("0", dataset_name="another-profile")) == 2

    # for the first run, verify content
    assert len(default_profiles) == 1
    assert len(another_profile) == 1
    #assert default_profiles[0].name == "default"
    assert default_profiles[0].dataset_timestamp is not None
    assert another_profile[0].dataset_timestamp is not None
    #whylogs.mlflow.disable_mlflow()
