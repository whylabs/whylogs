import datetime
import logging
import os
from typing import Dict, Optional

import pandas as pd

from whylogs import __version__ as whylogs_version
from whylogs import get_or_create_session
from whylogs.app.logger import Logger

logger = logging.getLogger(__name__)

_mlflow = None
_original_end_run = None
_active_whylogs = []
_is_patched = False
_original_mlflow_conda_env = None
_original_add_to_model = None
_original_model_log = None


class WhyLogsRun(object):
    _session = None
    _active_run_id = None
    _loggers: Dict[str, Logger] = dict()

    def __init__(self, session=None):
        logger.debug("Creating a real session for WhyLogsRun")
        self._session = session if session else get_or_create_session()

    def _create_logger(self, dataset_name: Optional[str] = None, dataset_timestamp: Optional[datetime.datetime] = None):
        active_run = _mlflow.active_run()
        if self._active_run_id is not None and active_run is None:
            self._close()
            self._active_run_id = None
            return None

        run_info = active_run.info
        if run_info.run_id != self._active_run_id:
            logger.debug(
                "Detect a new run ID. Previous run ID: %s. New run ID: %s.",
                self._active_run_id,
                run_info.run_id,
            )
            self._close()
            self._active_run_id = run_info.run_id
        session_timestamp = datetime.datetime.utcfromtimestamp(run_info.start_time / 1000.0)
        experiment: _mlflow.entities.Experiment = _mlflow.tracking.MlflowClient().get_experiment(run_info.experiment_id)
        logger_dataset_name = dataset_name or experiment.name
        tags = dict(active_run.data.tags)
        tags["mflow.experiment_id"] = active_run.info.experiment_id
        tags["mflow.run_id"] = active_run.info.run_id
        logger.debug(
            "Creating a new logger for dataset name: %s. Tags: %s",
            logger_dataset_name,
            tags,
        )
        logger_ = self._session.logger(run_info.run_id, session_timestamp=session_timestamp, dataset_timestamp=dataset_timestamp, tags=tags)
        return logger_

    def log_pandas(self, df: pd.DataFrame, dataset_name: Optional[str] = None, dataset_timestamp: Optional[datetime.datetime] = None):
        """
        Log the statistics of a Pandas dataframe. Note that this method is additive
        within a run: calling this method with a specific dataset name will not generate
        a new profile; instead, data will be aggregated into the existing profile.

        In order to create a new profile, please specify a dataset_name

        :param df: the Pandas dataframe to log
        :param dataset_name: the name of the dataset (Optional). If not specified, the experiment name is used
        """
        ylogs = self._get_or_create_logger(dataset_name, dataset_timestamp=dataset_timestamp)

        if ylogs is None:
            logger.warning("Unable to get an active logger. Are you in an active MLFlow run?")

        ylogs.log_dataframe(df)

    def log(
        self,
        features: Dict[str, any] = None,
        feature_name: str = None,
        value: any = None,
        dataset_name: Optional[str] = None,
    ):
        """
        Logs a collection of features or a single feature (must specify one or the other).

        :param features: a map of key value feature for model input
        :param feature_name: a dictionary of key->value for multiple features. Each entry represent a single columnar feature
        :param feature_name: name of a single feature. Cannot be specified if 'features' is specified
        :param value: value of as single feature. Cannot be specified if 'features' is specified
        :param dataset_name: the name of the dataset. If not specified, we fall back to using the experiment name
        """
        ylogs = self._get_or_create_logger(dataset_name)
        if ylogs is None:
            logger.warning("Unable to get an active logger. Are you in an active MLFlow run?")
            return

        ylogs.log(features, feature_name, value)

    def _get_or_create_logger(self, dataset_name: Optional[str] = None, dataset_timestamp: Optional[datetime.datetime] = None):
        ylogs = self._loggers.get(dataset_name)
        if ylogs is None:
            ylogs = self._create_logger(dataset_name, dataset_timestamp=dataset_timestamp)
            self._loggers[dataset_name] = ylogs
        return ylogs

    def _close(self):
        logger.debug("Attempting close patcher WhyLogsRun")
        for name in list(self._loggers.keys()):
            try:
                ylogs = self._loggers[name]
                ylogs.close()
                self._loggers.pop(name)
            except Exception as ex:  # noqa
                logger.warning(
                    "Exception happened when saving %s for run %s",
                    name,
                    self._active_run_id,
                )
        logger.debug("Finished uploading all the loggers")
        self._active_run_id = None
        logger.debug("Finished closing the session")


def _new_mlflow_conda_env(
    path=None,
    additional_conda_deps=None,
    additional_pip_deps=None,
    additional_conda_channels=None,
    install_mlflow=True,
):
    global _original_mlflow_conda_env
    pip_deps = additional_pip_deps or []
    pip_deps.append(f"whylogs=={whylogs_version}")
    return _original_mlflow_conda_env(path, additional_conda_deps, pip_deps, additional_conda_channels, install_mlflow)


def _new_add_to_model(model, loader_module, data=None, code=None, env=None, **kwargs):
    """
    Replaces the MLFLow's original add_to_model
    https://github.com/mlflow/mlflow/blob/4e68f960d4520ade6b64a28c297816f622adc83e/mlflow/pyfunc/__init__.py#L242

    Accepts the same signature as MLFlow's original add_to_model call. We inject our loader module.

    We also inject `whylogs` into the Conda environment by patching `_mlflow_conda_env`.

    :param model: Existing model.
    :param loader_module: The module to be used to load the model.
    :param data: Path to the model data.
    :param code: Path to the code dependencies.
    :param env: Conda environment.
    :param kwargs: Additional key-value pairs to include in the ``pyfunc`` flavor specification.
                   Values must be YAML-serializable.
    :return: Updated model configuration.
    """

    global _original_add_to_model
    patched_loader_module = loader_module
    # TODO: support more loader module
    if loader_module == "mlflow.sklearn":
        patched_loader_module = "whylogs.mlflow.sklearn"

    _original_add_to_model(model, patched_loader_module, data, code, env, **kwargs)


WHYLOG_YAML = ".whylogs.yaml"


def new_model_log(**kwargs):
    """
    Hijack the mlflow.models.Model.log method and upload the .whylogs.yaml configuration to the model path
    This will allow us to pick up the configuration later under /opt/ml/model/.whylogs.yaml path
    """
    import mlflow

    global _original_model_log

    if not os.path.isfile(WHYLOG_YAML):
        logger.warning("Unable to detect .whylogs.yaml file under current directory. whylogs will write to local disk in the " "container")
        _original_model_log(**kwargs)
        return
    if _original_model_log is None:
        raise RuntimeError("MlFlow is not patched. Please call whylogs.enable_mlflow()")
    mlflow.log_artifact(WHYLOG_YAML, kwargs["artifact_path"])
    _original_model_log(**kwargs)


def enable_mlflow(session=None) -> bool:
    """
    Enable whylogs in ``mlflow`` module via ``mlflow.whylogs``.

    :returns: True if MLFlow has been patched. False otherwise.

    .. code-block:: python
        :caption: Example of whylogs and MLFlow

        import mlflow
        import whylogs

        whylogs.enable_mlflow()

        import numpy as np
        import pandas as pd
        pdf = pd.DataFrame(
            data=[[1, 2, 3, 4, True, "x", bytes([1])]],
            columns=["b", "d", "a", "c", "e", "g", "f"],
            dtype=np.object,
        )

        active_run = mlflow.start_run()

        # log a Pandas dataframe under default name
        mlflow.whylogs.log_pandas(pdf)

        # log a Pandas dataframe with custom name
        mlflow.whylogs.log_pandas(pdf, "another dataset")

        # Finish the MLFlow run
        mlflow.end_run()

    """
    global _mlflow
    global _is_patched
    global _original_end_run
    global _original_mlflow_conda_env
    global _original_add_to_model
    global _original_model_log

    if _is_patched:
        logger.warning("whylogs has been enabled for MLFlow. Ignoring...")
        return True

    try:
        import mlflow

        _mlflow = mlflow
    except ImportError:
        logger.warning("Failed to import MLFlow. Please make sure MLFlow is installed in your runtime")
        return False
    _original_mlflow_conda_env = _mlflow.utils.environment._mlflow_conda_env
    _original_add_to_model = _mlflow.pyfunc.add_to_model
    _original_model_log = _mlflow.models.Model.log
    _original_end_run = _mlflow.tracking.fluent.end_run

    if len(_active_whylogs) > 0:
        ylogs = _active_whylogs[0]
    else:
        ylogs = WhyLogsRun(session)
        _active_whylogs.append(ylogs)

    _mlflow.whylogs = ylogs

    # Store the original end_run
    def end_run(
        status=_mlflow.entities.RunStatus.to_string(_mlflow.entities.RunStatus.FINISHED),
    ):
        logger.debug("Closing whylogs before ending the MLFlow run")
        _mlflow.whylogs._close()
        _original_end_run(status)

    _mlflow.utils.environment._mlflow_conda_env = _new_mlflow_conda_env
    _mlflow.pyfunc.add_to_model = _new_add_to_model
    _mlflow.end_run = end_run
    _mlflow.tracking.fluent.end_run = end_run
    _mlflow.models.Model.log = new_model_log

    try:
        import sys

        del sys.modules["mlflow.sklearn"]
        del sys.modules["mlflow.pyfunc"]
        del sys.modules["mlflow.tracking.fluent"]
        del sys.modules["mlflow.models"]
    except:  # noqa
        pass

    _is_patched = True

    return True


def disable_mlflow():
    global _mlflow
    global _is_patched
    global _original_end_run
    global _original_mlflow_conda_env
    global _original_add_to_model
    global _original_model_log

    try:
        import mlflow

        mlflow.end_run()
        mlflow.end_run = _original_end_run
        mlflow.tracking.fluent.end_run = _original_end_run
        mlflow.utils.environment._mlflow_conda_env = _original_mlflow_conda_env
        mlflow.pyfunc.add_to_model = _original_add_to_model
        mlflow.models.Model.log = _original_model_log
        del mlflow.whylogs
    except:  # noqa
        pass

    _mlflow = None
    _is_patched = False
