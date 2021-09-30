from .patcher import disable_mlflow, enable_mlflow

_WHYLOGS_PATH = "whylogs"


def list_whylogs_runs(experiment_id: str, dataset_name: str = "default"):
    """
    List all the runs from an experiment that contains whylogs

    :rtype: :py:class:`typing.List[mlflow.entities.Run]`
    :param experiment_id: the experiment id
    :param dataset_name: the name of the dataset. Default to "default"
    """

    import mlflow

    client = mlflow.tracking.MlflowClient()
    run_infos = client.list_run_infos(experiment_id)

    res = []
    for run in run_infos:
        if run.status != "FINISHED":
            continue
        artifacts = client.list_artifacts(run.run_id, path=f"{_WHYLOGS_PATH}/{run.run_id}")
        if len(artifacts) == 1 and not artifacts[0].is_dir and artifacts[0].path.endswith("/profile.bin"):
            res.append(run)
    return res


def get_run_profiles(run_id: str, dataset_name: str = "default", client=None):
    """
    Retrieve all whylogs DatasetProfile for a given run and a given dataset name.

    :param client: :py:class:`mlflow.tracking.MlflowClient`
    :rtype: :py:class:`typing.List[whylogs.DatasetProfile]`
    :param run_id: the run id
    :param dataset_name: the dataset name within a run. If not set, use the default value "default"
    """

    import shutil
    import tempfile

    import mlflow

    from whylogs import DatasetProfile

    if client is None:
        client = mlflow.tracking.MlflowClient()

    artifacts = client.list_artifacts(run_id, path=f"{_WHYLOGS_PATH}/{run_id}")
    if len(artifacts) == 1 and not artifacts[0].is_dir:
        tmp_dir = tempfile.mkdtemp()
        output_file = client.download_artifacts(run_id, artifacts[0].path, tmp_dir)
        try:
            with open(output_file, "rb") as f:
                return list(DatasetProfile.parse_delimited(f.read()))
        finally:
            shutil.rmtree(tmp_dir)
    else:
        return []


def get_experiment_profiles(experiment_id: str, dataset_name: str = "default"):
    """
    Retrieve all whylogs profiles for a given experiment. This only
    returns Active Runs at the moment.

    :rtype: :py:class:`typing.List[whylogs.DatasetProfile]`
    :param experiment_id: the experiment ID string
    :param dataset_name: the dataset name within a run. If not set, use the default value "default"
    """
    import mlflow

    client = mlflow.tracking.MlflowClient()
    run_infos = list_whylogs_runs(experiment_id, dataset_name)

    res = []
    for run in run_infos:
        res.extend(get_run_profiles(run.run_id, dataset_name, client))
    return res


__all__ = [
    "enable_mlflow",
    "disable_mlflow",
    "get_experiment_profiles",
    "get_run_profiles",
    "list_whylogs_runs",
]
