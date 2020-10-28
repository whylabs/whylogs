"""
whylogs logging session
"""
import datetime
from logging import getLogger as _getLogger
from typing import Dict, List, Optional
from uuid import uuid4

import pandas as pd

from whylogs.app.config import SessionConfig, WriterConfig, load_config
from whylogs.app.logger import Logger
from whylogs.app.writers import Writer, writer_from_config
from whylogs.core import DatasetProfile


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
        self, project: str, pipeline: str, writers: List[Writer], verbose: bool = False,
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
        self._session_id = str(uuid4())

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
        tags: Dict[str, str] = None,
        metadata: Dict[str, str] = None,
    ) -> Logger:
        """
        Create a new logger or return an existing one for a given dataset name.
        If no dataset_name is specified, we default to project name

        Parameters
        ----------
        metadata
        dataset_name : str
            Name of the dataset. Default is the project name
        dataset_timestamp: datetime.datetime, optional
            The timestamp associated with the dataset. Could be the timestamp
            for the batch, or the timestamp
            for the window that you are tracking
        tags: dict
            Tag the data with groupable information. For example, you might want to tag your data
            with the stage information (development, testing, production etc...)
        metadata: dict
            Useful to debug the data source. You can associate non-groupable information in this field
            such as hostname,
        session_timestamp: datetime.datetime, optional
            Override the timestamp associated with the session. Normally you
            shouldn't need to override this value
        Returns
        -------
        ylog : whylogs.app.logger.Logger
            whylogs logger
        """
        if not self._active:
            raise RuntimeError("Session is already closed. Cannot create more loggers")

        if dataset_name is None:
            # using the project name for the datasetname
            dataset_name = self.project
        if session_timestamp is None:
            session_timestamp = self._session_time

        # remove inactive loggers first
        for name, logger in list(self._loggers.items()):
            if not logger.is_active():
                self._loggers.pop(name)

        logger = self._loggers.get(dataset_name)
        if logger is None:
            logger = Logger(
                session_id=self._session_id,
                dataset_name=dataset_name,
                dataset_timestamp=dataset_timestamp,
                session_timestamp=session_timestamp,
                writers=self.writers,
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
        dataset_timestamp: Optional[datetime.datetime] = None,
        session_timestamp: Optional[datetime.datetime] = None,
        tags: Dict[str, str] = None,
        metadata: Dict[str, str] = None,
    ) -> Optional[DatasetProfile]:
        """
        Perform statistics caluclations and log a pandas dataframe

        :param df: the dataframe to profile 
        :param dataset_name: name of the dataset
        :param dataset_timestamp: the timestamp for the dataset
        :param session_timestamp: the timestamp for the session. Override the default one
        :param tags: the tags for the profile. Useful when merging
        :param metadata: information about this current profile. Can be discarded when merging
        :return: a dataset profile if the session is active
        """
        if not self.is_active():
            return None

        if dataset_name is None:
            # using the project name for the datasetname
            dataset_name = self.project

        ylog = self.logger(
            dataset_name, dataset_timestamp, session_timestamp, tags, metadata
        )

        ylog.log_dataframe(df)

        return ylog.close()

    def profile_dataframe(
        self,
        df: pd.DataFrame,
        dataset_name: Optional[str] = None,
        dataset_timestamp: Optional[datetime.datetime] = None,
        session_timestamp: Optional[datetime.datetime] = None,
        tags: Dict[str, str] = None,
        metadata: Dict[str, str] = None,
    ) -> Optional[DatasetProfile]:
        """
        Profile a Pandas dataframe without actually writing data to disk.
        This is useful when you just want to quickly capture and explore a dataset profile.

        :param df: the dataframe to profile
        :param dataset_name: name of the dataset
        :param dataset_timestamp: the timestamp for the dataset
        :param session_timestamp: the timestamp for the session. Override the default one
        :param tags: the tags for the profile. Useful when merging
        :param metadata: information about this current profile. Can be discarded when merging
        :return: a dataset profile if the session is active
        """
        dataset_profile = self.new_profile(
            dataset_name, dataset_timestamp, session_timestamp, tags, metadata
        )

        if dataset_profile is None:
            return None

        dataset_profile.track_dataframe(df)

        return dataset_profile

    def new_profile(
        self,
        dataset_name: Optional[str] = None,
        dataset_timestamp: Optional[datetime.datetime] = None,
        session_timestamp: Optional[datetime.datetime] = None,
        tags: Dict[str, str] = None,
        metadata: Dict[str, str] = None,
    ) -> Optional[DatasetProfile]:
        """
        Create an empty dataset profile with the metadata from the session.

        :param dataset_name: name of the dataset
        :param dataset_timestamp: the timestamp for the dataset
        :param session_timestamp: the timestamp for the session. Override the default one
        :param tags: the tags for the profile. Useful when merging
        :param metadata: information about this current profile. Can be discarded when merging
        :return: a dataset profile if the session is active
        """
        if not self.is_active():
            return None

        if not self._active:
            raise RuntimeError("Session is already closed. Cannot create more loggers")

        if dataset_name is None:
            # using the project name for the datasetname
            dataset_name = self.project
        if session_timestamp is None:
            session_timestamp = self._session_time

        if tags is None:
            tags = dict()
        if self.pipeline:
            tags["Pipeline"] = self.pipeline

        profile = DatasetProfile(
            dataset_name,
            dataset_timestamp=dataset_timestamp,
            session_timestamp=session_timestamp,
            tags=tags,
            metadata=metadata,
        )

        return profile

    def close(self):
        """
        Deactivate this session and flush all associated loggers
        """
        if not self._active:
            print("WARNING: attempting to close an inactive session")
            return

        self._active = False
        loggers = list(self._loggers.items())
        for name, logger in loggers:
            if logger.is_active():
                logger.close()
            self.remove_logger(name)

    def remove_logger(self, dataset_name):
        """
        Remove a logger from the dataset. This is called by the logger when it's being closed

        Parameters
        ----------
        dataset_name the name of the dataset. used to identify the logger

        Returns None
        -------

        """
        if self._loggers.get(dataset_name) is None:
            print(
                "WARNING: logger {} is not present in the current Session".format(
                    dataset_name
                )
            )
            return

        self._loggers.pop(dataset_name)


def session_from_config(config: SessionConfig) -> Session:
    """
    Construct a whylogs session from a `SessionConfig`
    """
    writers = list(map(lambda x: writer_from_config(x), config.writers))
    return Session(config.project, config.pipeline, writers, config.verbose)


#: A global session
_session = None


def reset_default():
    """
    DEPRECATED. Please use reset_default_session()
    """
    from warnings import warn

    warn("DEPRECATED. use reset_default_session() instead of reset_default()")
    reset_default_session()


def reset_default_session():
    """
    Reset and deactivate the global whylogs logging session.
    """
    global _session
    if _session is not None:
        _session.close()
    config: SessionConfig = load_config()
    if config is None:
        raise EnvironmentError("Unable to load whylogs config")
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
