"""
"""
import datetime
from logging import getLogger as _getLogger
from typing import List, Optional, Dict, Any

import pandas as pd
import typing

from whylogs.app.config import SessionConfig, WriterConfig, load_config
from whylogs.app.logger import Logger
from whylogs.app.writers import Writer, writer_from_config


class Session:
    _loggers: Dict[str, Logger]

    def __init__(
        self, project: str, pipeline: str, writers: List[Writer], verbose: bool = False,
    ):
        """

        Parameters
        ----------
        project : the project name. We will default to the project name when logging a dataset
         if the dataset name is not specified
        writers : configuration for the output writers. This is where the log data will go
        verbose : enable verbose logging for not. Default is False
        """
        if writers is None:
            writers = []
        self.project = project
        self.pipeline = pipeline
        self.writers = writers
        self.verbose = verbose
        self._active = True
        self._loggers = dict()
        self._session_time = datetime.datetime.now()

    def __enter__(self):
        # TODO: configure other aspects
        return self

    def __exit__(self, tpe, value, traceback):
        self.close()

    @property
    def active(self):
        return self._active

    def logger(
        self,
        dataset_name: Optional[str] = None,
        dataset_timestamp: Optional[datetime.datetime] = None,
        session_timestamp: Optional[datetime.datetime] = None,
        tags: Optional[typing.Dict[str, str]] = None,
        metadata: Optional[typing.Dict[str, str]] = None,
    ) -> Logger:
        """
        Create a new logger or return an existing one for a given dataset name.
        If no dataset_name is specified, we default to project name

        Parameters
        ----------
        tags:
            Map of key=value strings that can be used to group/aggregate metrics
        metadata:
            Useful for auditing the source of the data (i.e. the host that generated the data, or the user
            that ran it).
        dataset_name :
            Name of the dataset. Default is the project name
        dataset_timestamp:
            The timestamp associated with the dataset. Could be the timestamp for the batch, or the timestamp
            for the window that you are tracking
        session_timestamp
            Override the timestamp associated with the session. Normally you shouldn't need to override this value
        Returns
        -------

        """
        if dataset_name is None:
            # using the project name for the datasetname
            dataset_name = self.project
        if session_timestamp is None:
            session_timestamp = self._session_time

        if not self._active:
            raise RuntimeError("Session is already closed. Cannot create more loggers")
        logger = self._loggers.get(dataset_name)

        if logger is None:
            if tags is None:
                tags = dict()
            if self.project is not None:
                tags["Project"] = self.project
            if self.pipeline is not None:
                tags["Pipeline"] = self.pipeline

            logger = Logger(
                dataset_name=dataset_name,
                writers=self.writers,
                dataset_timestamp=dataset_timestamp,
                session_timestamp=session_timestamp,
                tags=tags,
                metadata=metadata,
                verbose=self.verbose,
            )
            self._loggers[dataset_name] = logger

        return logger

    def log_dataframe(
        self,
        df: pd.DataFrame,
        dataset_name: Optional[str] = None,
        datetime_column: Optional[str] = None,
        datetime_format: Optional[str] = None,
        tags: Optional[typing.Dict[str, str]] = None,
        metadata: Optional[typing.Dict[str, str]] = None,
    ):
        if not self.active:
            return

        logger = self._loggers.get(dataset_name)
        if logger is None:
            with self.logger(
                dataset_name, datetime_column, datetime_format, tags, metadata
            ) as logger:
                logger.log_dataframe(df)
        else:
            logger.log_dataframe(df)

    def close(self):
        if not self._active:
            print("WARNING: attempting to close an inactive session")
            return

        self._active = False
        for name, logger in self._loggers.items():
            if logger.is_active():
                logger.close()


def session_from_config(config: SessionConfig) -> Session:
    writers = list(map(lambda x: writer_from_config(x), config.writers))
    return Session(config.project, config.pipeline, writers, config.verbose)


# Create a global session
_session = None


def reset_default():
    """
    Reset and inactivate the logging session.
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
    Retrieve the current active session.  If no active session is found,
    create the session.
    """
    global _session
    if _session is not None and _session.active:
        _getLogger(__name__).debug("Active session found, ignoring session kwargs")
    else:
        config = load_config()
        if config is None:
            print("WARN: Missing config")
            writer = WriterConfig(type="local", output_path="output", formats=["all"])
            config = SessionConfig(
                "default-project", "default-pipeline", [writer], False
            )
        _session = session_from_config(config)
    return _session


def get_session():
    """
    Retrieve the logging session without altering or activating it.
    """
    return _session


def get_logger():
    """
    Retrieve the logger.
    """
    return _session.logger
