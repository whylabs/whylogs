"""
"""
from logging import getLogger as _getLogger
from typing import Optional, List

import pandas as pd

from whylabs.logs.app.config import SessionConfig
from whylabs.logs.app.logger import Logger
from whylabs.logs.app.writers import Writer, writer_from_config


class Session:
    def __init__(self,
                 project: str,
                 pipeline: str,
                 writers: List[Writer],
                 verbose: bool = False,
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
        self._loggers = {}

    def __enter__(self):
        # TODO: configure other aspects
        return self

    def __exit__(self, tpe, value, traceback):
        self.close()

    def logger(self,
               dataset_name: Optional[str] = None,
               datetime_column: Optional[str] = None,
               datetime_format: Optional[str] = None) -> Logger:
        """
        Create a new logger or return an existing one for a given dataset name.
        If no dataset_name is specified, we default to project name

        Parameters
        ----------
        dataset_name :
            Name of the dataset. Default is the project name
        datetime_column
        datetime_format :
            Optional.
        Returns
        -------

        """
        if dataset_name is None:
            # using the project name for the datasetname
            dataset_name = self.project

        if not self._active:
            raise RuntimeError('Session is already closed. Cannot create more loggers')
        logger = self._loggers.get(dataset_name)
        if logger is None:
            logger = Logger(dataset_name=dataset_name,
                            datetime_column=datetime_column,
                            datetime_format=datetime_format,
                            writers=self.writers,
                            verbose=self.verbose,
                            )
            self._loggers[dataset_name] = logger

        return logger

    def log_dataframe(self,
                      df: pd.DataFrame,
                      dataset_name: Optional[str] = None,
                      datetime_column: Optional[str] = None,
                      datetime_format: Optional[str] = None):
        if not self.is_active():
            return

        with self.logger(dataset_name, datetime_column, datetime_format) as logger:
            logger.log_dataframe(df)

    def close(self):
        if not self._active:
            print('WARNING: attempting to close an inactive session')
            return

        self._active = False
        for name, logger in self._loggers.items():
            if logger.is_active():
                logger.close()

    def is_active(self):
        return self._active


def session_from_config(config: SessionConfig) -> Session:
    writers = map(lambda x: writer_from_config(x), config.writers)
    return Session(config.project, writers, config.verbose)


# Create a global session
_session = None


def reset_default():
    """
    Reset and inactivate the logging session.
    """
    global _session
    _session.close()
    _session = Session()


def get_or_create_session(**kwargs):
    """
    Retrieve the current active session.  If no active session is found,
    create the session.

    Parameters
    ----------
    kwargs:
        Session configuration, passed to the session.  These are ignored
        if an active session is already found.

    Returns
    -------
    session : Session
        The active session
    """
    global _session
    if _session.is_active():
        _getLogger(__name__).debug(
            'Active session found, ignoring session kwargs')
    else:
        _session = Session(**kwargs)
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
