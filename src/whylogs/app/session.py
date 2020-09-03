"""
WhyLogs logging session
"""
import datetime
from logging import getLogger as _getLogger
from typing import List, Optional

import pandas as pd

from whylogs.app.config import SessionConfig, WriterConfig, load_config
from whylogs.app.logger import Logger
from whylogs.app.writers import Writer, writer_from_config


class Session:
    """
    Parameters
    ----------
    project : str
        The project name. We will default to the project name when logging
        a dataset if the dataset name is not specified
    pipeline : str
        Name of the pipeline associated with this session
    writers : list
        configuration for the output writers. This is where the log data
        will go
    verbose : bool
        enable verbose logging for not. Default is ``False``
    """
    def __init__(
            self, project: str, pipeline: str, writers: List[Writer],
            verbose: bool = False,
        ):
        if writers is None:
            writers = []
        self.project = project
        self.pipeline = pipeline
        self.writers = writers
        self.verbose = verbose
        self._active = True
        self._loggers = {}
        self._session_time = datetime.datetime.now()

    def __enter__(self):
        # TODO: configure other aspects
        return self

    def __exit__(self, tpe, value, traceback):
        self.close()

    def is_active(self):
        return self._active

    def logger(
            self,
            dataset_name: Optional[str] = None,
            dataset_timestamp: Optional[datetime.datetime] = None,
            session_timestamp: Optional[datetime.datetime] = None,
        ) -> Logger:
        """
        Create a new logger or return an existing one for a given dataset name.
        If no dataset_name is specified, we default to project name

        Parameters
        ----------
        dataset_name : str
            Name of the dataset. Default is the project name
        dataset_timestamp: datetime.datetime, optional
            The timestamp associated with the dataset. Could be the timestamp
            for the batch, or the timestamp
            for the window that you are tracking
        session_timestamp: datetime.datetime, optional
            Override the timestamp associated with the session. Normally you
            shouldn't need to override this value
        Returns
        -------
        ylog : whylogs.app.logger.Logger
            WhyLogs logger
        """
        if dataset_name is None:
            # using the project name for the datasetname
            dataset_name = self.project
        if session_timestamp is None:
            session_timestamp = self._session_time

        if not self._active:
            raise RuntimeError(
                "Session is already closed. Cannot create more loggers")
        logger = self._loggers.get(dataset_name)
        if logger is None:
            logger = Logger(
                dataset_name=dataset_name,
                dataset_timestamp=dataset_timestamp,
                session_timestamp=session_timestamp,
                writers=self.writers,
                verbose=self.verbose,
            )
            self._loggers[dataset_name] = logger

        return logger

    def log_dataframe(
            self,
            df: pd.DataFrame,
            dataset_name: Optional[str] = None,
            datetime_column: Optional = None,
            datetime_format: Optional[str] = None,
        ):
        """
        Perform statistics caluclations and log a pandas dataframe

        Parameters
        ----------
        df : pd.DataFrame
            Pandas DataFrame
        dataset_name : str
            Name of the dataset
        datetime_column : hash-able
            Specify a column to be parsed as a datetime which can be used to
            split the dataframe into multiple batches
        datetime_format : str
            Specify a datetime format.  See `datetime.strftime`
        """
        if not self.is_active():
            return

        with self.logger(dataset_name, datetime_column, datetime_format) as logger:
            logger.log_dataframe(df)

    def close(self):
        """
        Deactivate this session and flush all associated loggers
        """
        if not self._active:
            print("WARNING: attempting to close an inactive session")
            return

        self._active = False
        for name, logger in self._loggers.items():
            if logger.is_active():
                logger.close()

    def is_active(self):
        """
        Return the boolean state of this Session
        """
        return self._active


def session_from_config(config: SessionConfig) -> Session:
    """
    Construct a WhyLogs session from a `SessionConfig`
    """
    writers = list(map(lambda x: writer_from_config(x), config.writers))
    return Session(config.project, config.pipeline, writers, config.verbose)


#: A global session
_session = None


def reset_default():
    """
    Reset and inactivate the global WhyLogs logging session.
    """
    global _session
    if _session is not None:
        _session.close()
    config: SessionConfig = load_config()
    if config is None:
        raise EnvironmentError("Unable to load WhyLogs config")
    _session = session_from_config(config)


def get_or_create_session():
    """
    Retrieve the current active global session.

    If no active session exists, attempt to load config and create a new
    session.

    If an active session exists, return the session without loading new
    config.

    Returns
    -------
    session : Session
        The global active session
    """
    global _session
    if _session is not None and _session.is_active():
        _getLogger(__name__).debug(
            "Active session found, ignoring session kwargs")
    else:
        config = load_config()
        if config is None:
            print("WARN: Missing config")
            writer = WriterConfig(type="local", output_path="output",
                                  formats=["all"])
            config = SessionConfig(
                "default-project", "default-pipeline", [writer], False
            )
        _session = session_from_config(config)
    return _session


def get_session():
    """
    Retrieve the logging session without altering or activating it.

    Returns
    -------
    session : Session
        The global session
    """
    return _session


def get_logger():
    """
    Retrieve the global session logger

    Returns
    -------
    ylog : whylogs.app.logger.Logger
        The global session logger
    """
    return _session.logger
