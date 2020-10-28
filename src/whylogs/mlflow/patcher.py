import datetime
import logging
import os
import tempfile
from typing import Optional, Dict

import pandas as pd

from whylogs.app.logger import Logger

logger = logging.getLogger("whylogs.mlflow")

_mlflow = None
_original_end_run = None
_active_whylogs = []
_is_patched = False


class WhyLogsRun(object):
    _session = None
    _active_run_id = None
    _loggers: Dict[str, Logger] = dict()

    def _create_logger(self, dataset_name: Optional[str] = None):
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

        session_timestamp = datetime.datetime.utcfromtimestamp(
            run_info.start_time / 1000.0
        )
        experiment: _mlflow.entities.Experiment = _mlflow.tracking.MlflowClient().get_experiment(
            run_info.experiment_id
        )
        logger_dataset_name = dataset_name or experiment.name
        tags = dict(active_run.data.tags)
        tags["mflow.experiment_id"] = active_run.info.experiment_id
        tags["mflow.run_id"] = active_run.info.run_id
        logger.debug(
            "Creating a new logger for dataset name: %s. Tags: %s",
            logger_dataset_name,
            tags,
        )
        return Logger(
            run_info.run_id,
            logger_dataset_name,
            session_timestamp=session_timestamp,
            dataset_timestamp=session_timestamp,
            tags=tags,
            writers=[],
        )

    def log_pandas(self, df: pd.DataFrame, dataset_name: Optional[str] = None):
        """
        Log the statistics of a Pandas dataframe. Note that this method is additive
        within a run: calling this method with a specific dataset name will not generate
        a new profile; instead, data will be aggregated into the existing profile.

        In order to create a new profile, please specify a dataset_name

        :param df: the Pandas dataframe to log
        :param dataset_name: the name of the dataset (Optional). If not specified, the experiment name is used
        """
        ylogs = self._get_or_create_logger(dataset_name)

        if ylogs is None:
            logger.warning(
                "Unable to get an active logger. Are you in an active MLFlow run?"
            )

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
            logger.warning(
                "Unable to get an active logger. Are you in an active MLFlow run?"
            )
            return

        ylogs.log(features, feature_name, value)

    def _get_or_create_logger(self, dataset_name: Optional[str] = None):
        ylogs = self._loggers.get(dataset_name)
        if ylogs is None:
            ylogs = self._create_logger(dataset_name)
            self._loggers[dataset_name] = ylogs
        return ylogs

    def _close(self):
        tmp_dir = tempfile.mkdtemp()
        logger.debug("Using tmp dir: %s", tmp_dir)
        for name in list(self._loggers.keys()):
            try:
                ylogs = self._loggers[name]
                dataset_dir = name or "default"
                output_dir = os.path.join(tmp_dir, dataset_dir)
                os.makedirs(output_dir, exist_ok=True)
                output = os.path.join(output_dir, "profile.bin")
                logger.debug("Writing logger %s's data to %s", name, output)
                ylogs.profile.write_protobuf(output)
                _mlflow.log_artifact(output, artifact_path=f"whylogs/{dataset_dir}")
                logger.debug("Successfully uploaded logger %s data to MLFlow", name)
                self._loggers.pop(name)
            except:
                logger.warning(
                    "Exception happened when saving %s for run %s",
                    name,
                    self._active_run_id,
                )
                pass
            logger.debug("Finished uploading all the loggers")
            self._active_run_id = None


def enable_mlflow() -> bool:
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

    if _is_patched:
        logger.warning("whylogs has been enabled for MLFlow. Ignoring...")
        return True

    try:
        import mlflow

        _mlflow = mlflow
    except ImportError:
        logger.warning(
            "Failed to import MLFlow. Please make sure MLFlow is installed in your runtime"
        )
        return False

    if len(_active_whylogs) > 0:
        ylogs = _active_whylogs[0]
    else:
        ylogs = WhyLogsRun()
        _active_whylogs.append(ylogs)

    _mlflow.whylogs = ylogs
    # Store the original end_run
    _original_end_run = _mlflow.tracking.fluent.end_run

    def end_run(
        status=_mlflow.entities.RunStatus.to_string(
            _mlflow.entities.RunStatus.FINISHED
        ),
    ):
        logger.debug("Closing whylogs before ending the MLFlow run")
        _mlflow.whylogs._close()
        _original_end_run(status)

    _mlflow.end_run = end_run
    _mlflow.tracking.fluent.end_run = end_run
    _is_patched = True

    return True


def disable_mlflow():
    try:
        import mlflow

        mlflow.end_run()
        mlflow.end_run = _original_end_run
        mlflow.tracking.fluent.end_run = _original_end_run
        del mlflow.whylogs
    except:
        pass

    global _mlflow
    global _is_patched
    _mlflow = None
    _is_patched = False
