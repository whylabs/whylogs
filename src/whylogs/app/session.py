"""
whylogs logging session
"""
import datetime
import warnings
from dataclasses import dataclass
from logging import getLogger as _getLogger
from typing import Dict, List, Optional, Union
from uuid import uuid4

import pandas as pd
from tqdm.auto import tqdm

from whylogs.app.config import MetadataConfig, SessionConfig, WriterConfig, load_config
from whylogs.app.logger import Logger
from whylogs.app.metadata_writer import MetadataWriter, metadata_from_config
from whylogs.app.writers import WhyLabsWriter, Writer, writer_from_config
from whylogs.core import DatasetProfile
from whylogs.core.statistics.constraints import DatasetConstraints
from whylogs.features.autosegmentation import _estimate_segments


@dataclass
class _LoggerKey:
    """
    Create a new logger or return an existing one for a given dataset name.
    If no dataset_name is specified, we default to project name

    Args:
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
        segments:
            Can be either:
            - List of tag key value pairs for tracking datasetments
            - List of tag keys for whylogs to split up the data in the backend
    """

    dataset_name: Optional[str] = None
    dataset_timestamp: Optional[datetime.datetime] = None
    session_timestamp: Optional[datetime.datetime] = None
    tags: Dict[str, str] = None
    metadata: Dict[str, str] = None
    segments: Optional[Union[List[Dict], List[str]]] = None
    profile_full_dataset: bool = False
    with_rotation_time: str = None
    cache_size: int = 1
    constraints: DatasetConstraints = None


defaultLoggerArgs = _LoggerKey()


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
        self,
        project: Optional[str] = None,
        pipeline: Optional[str] = None,
        writers: Optional[List[Writer]] = None,
        metadata_writer: Optional[MetadataWriter] = None,
        verbose: bool = False,
        with_rotation_time: str = None,
        cache_size: int = None,
        report_progress: bool = False,
    ):
        self._py_logger = _getLogger(__name__)
        if writers is None:
            writers = []
        if project is None:
            project = ""
        if pipeline is None:
            pipeline = ""
        self.project = project
        self.pipeline = pipeline
        self.writers = writers
        self.metadata_writer = metadata_writer
        self.verbose = verbose
        self._active = True
        self._loggers = {}
        self._session_time = datetime.datetime.now()
        self._session_id = str(uuid4())
        self._config = SessionConfig(project, pipeline, writers, metadata_writer, verbose)
        self.with_rotation_time = with_rotation_time
        self.cache_size = cache_size
        self.report_progress = report_progress

        # enable special logic when starting/closing a Session if we're using whylabs client to save dataset profiles
        whylabs_writer_is_present = any(isinstance(w, WhyLabsWriter) for w in self.writers)
        self.use_whylabs_writer = _use_whylabs_client or whylabs_writer_is_present

        # add WhyLabs writer if it's not already present (which can happen if it's not specified in the config)
        if _use_whylabs_client and whylabs_writer_is_present is False:
            self.writers.append(WhyLabsWriter(output_path=None, formats=["protobuf"]))

    def __enter__(self):
        if self.use_whylabs_writer:
            from whylogs.whylabs_client.wrapper import start_session

            start_session()
        return self

    def __exit__(self, tpe, value, traceback):
        self.close()

    def __repr__(self):
        return self._config.to_yaml()

    def get_config(
        self,
    ):
        return self._config

    def is_active(self):
        return self._active

    def logger(
        self,
        dataset_name: Optional[str] = None,
        dataset_timestamp: Optional[datetime.datetime] = None,
        session_timestamp: Optional[datetime.datetime] = None,
        tags: Dict[str, str] = None,
        metadata: Dict[str, str] = None,
        segments: Optional[Union[List[Dict], List[str], str]] = None,
        profile_full_dataset: bool = False,
        with_rotation_time: str = None,
        cache_size: int = 1,
        constraints: DatasetConstraints = None,
    ) -> Logger:
        """
        Create a new logger or return an existing one for a given dataset name.
        If no dataset_name is specified, we default to project name

        Args:
            dataset_name: name of the dataset
            dataset_timestamp: timestamp of the dataset. Default to now
            session_timestamp: timestamp of the session. Inherits from the session
            tags: metadata associated with the profile
            metadata: same as tags. Will be deprecated
            segments: slice of data that the profile belongs to
            profile_full_dataset: when segmenting dataset, an option to keep the full unsegmented profile of the dataset
            with_rotation_time: rotation time in minutes our hours ("1m", "1h")
            cache_size: size of the segment cache
            constraints: whylogs contrainst to monitor against
        """
        if not self._active:
            raise RuntimeError("Session is already closed. Cannot create more loggers")

        # Explicitly set the default timezone to utc if none was provided. Helps with equality testing
        # profiles by making sure dates are the same whether they're supplied or deserizlied from protobuf.
        if dataset_timestamp is not None and dataset_timestamp.tzinfo is None:
            dataset_timestamp = dataset_timestamp.replace(tzinfo=datetime.timezone.utc)

        logger_key = str(
            _LoggerKey(
                dataset_name=dataset_name,
                dataset_timestamp=dataset_timestamp,
                session_timestamp=session_timestamp,
                tags=tags,
                metadata=metadata,
                segments=segments,
                profile_full_dataset=profile_full_dataset,
                with_rotation_time=with_rotation_time,
                cache_size=cache_size,
                constraints=constraints,
            )
        )
        logger = self._loggers.get(logger_key)

        if logger is None or not logger.is_active():
            logger = Logger(
                session_id=self._session_id,
                dataset_name=dataset_name or self.project,
                dataset_timestamp=dataset_timestamp,
                session_timestamp=session_timestamp or self._session_time,
                writers=self.writers,
                metadata_writer=self.metadata_writer,
                tags=tags or {},
                metadata=metadata,
                verbose=self.verbose,
                with_rotation_time=with_rotation_time or self.with_rotation_time,
                segments=segments,
                profile_full_dataset=profile_full_dataset,
                cache_size=cache_size,
                constraints=constraints,
            )
            self._loggers[logger_key] = logger

        return logger

    def get_logger(self, dataset_name: str = None):
        return self._loggers.get(dataset_name, None)

    def log_dataframe(
        self,
        df: pd.DataFrame,
        dataset_name: Optional[str] = None,
        dataset_timestamp: Optional[datetime.datetime] = None,
        session_timestamp: Optional[datetime.datetime] = None,
        tags: Dict[str, str] = None,
        metadata: Dict[str, str] = None,
        segments: Optional[Union[List[Dict], List[str], str]] = None,
        profile_full_dataset: bool = False,
        constraints: DatasetConstraints = None,
    ) -> Optional[DatasetProfile]:
        """
        Perform statistics caluclations and log a pandas dataframe

        :param df: the dataframe to profile
        :param dataset_name: name of the dataset
        :param dataset_timestamp: the timestamp for the dataset
        :param session_timestamp: the timestamp for the session. Override the default one
        :param tags: the tags for the profile. Useful when merging
        :param metadata: information about this current profile. Can be discarded when merging
        :param segments:
            Can be either:
            - Autosegmentation source, one of ["auto", "local"]
            - List of tag key value pairs for tracking data segments
            - List of tag keys for which we will track every value
            - None, no segments will be used
        :param profile_full_dataset: when segmenting dataset, an option to keep the full unsegmented profile of the dataset
        :return: a dataset profile if the session is active
        """
        if not self.is_active():
            return None

        if dataset_name is None:
            # using the project name for the datasetname
            dataset_name = self.project

        ylog = self.logger(
            dataset_name,
            dataset_timestamp,
            session_timestamp,
            tags,
            metadata,
            segments=segments,
            profile_full_dataset=profile_full_dataset,
            constraints=constraints,
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
        dataset_profile = self.new_profile(dataset_name, dataset_timestamp, session_timestamp, tags, metadata)

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

        if dataset_name is None:
            # using the project name for the dataset name
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

    def estimate_segments(
        self,
        df: pd.DataFrame,
        name: str,
        target_field: str = None,
        max_segments: int = 30,
        dry_run: bool = False,
    ) -> Optional[Union[List[Dict], List[str]]]:
        """
        Estimates the most important features and values on which to segment
        data profiling using entropy-based methods.

        :param df: the dataframe of data to profile
        :param name: name for discovery in the logger, automatically applied
        to loggers with same dataset_name
        :param target_field: target field (optional)
        :param max_segments: upper threshold for total combinations of segments,
        default 30
        :param dry_run: run calculation but do not write results to metadata
        :return: a list of segmentation feature names
        """
        segments = _estimate_segments(df=df, target_field=target_field, max_segments=max_segments)

        if not dry_run:
            if self.metadata_writer:
                self.metadata_writer.autosegmentation_write(name, segments)

        return segments

    def close(self):
        """
        Deactivate this session and flush all associated loggers
        """
        if not self._active:
            self._py_logger.warning("attempting to close an inactive session")
            return

        self._active = False
        loggers = list(self._loggers.items())
        with tqdm(loggers, disable=self.report_progress is False) as t:
            for key, logger in t:
                t.set_description("Closing session")
                if logger.is_active():
                    logger.close()
                self.remove_logger(key)

        for w in self.writers:
            w.close()

        if self.use_whylabs_writer:
            from whylogs.whylabs_client.wrapper import end_session

            url = end_session()
            if url:
                print(f"You can explore your data in the WhyLabs Platform here: {url}")

    def remove_logger(self, dataset_name: str):
        """
        Remove a logger from the dataset. This is called by the logger when it's being closed

        Parameters
        ----------
        dataset_name the name of the dataset. used to identify the logger

        Returns None
        -------

        """
        if self._loggers.get(dataset_name) is None:
            raise KeyError("WARNING: logger {} is not present in the current Session".format(dataset_name))

        self._loggers.pop(dataset_name)


#: Global flag for whether whylabs client should be used
_use_whylabs_client = False


def session_from_config(config: SessionConfig = None, config_path: Optional[str] = "") -> Session:
    """
    Construct a whylogs session from a `SessionConfig` or from a config_path
    """

    if config is None:
        config = load_config(config_path)

    writers = list(map(lambda x: writer_from_config(x), config.writers))
    metadata_writer = None
    if config.metadata:
        metadata_writer = metadata_from_config(config.metadata)
    return Session(
        config.project,
        config.pipeline,
        writers,
        metadata_writer,
        config.verbose,
        config.with_rotation_time,
        config.cache_size,
        report_progress=config.report_progress,
    )


#: A global session
_session = None


def reset_default_session():
    """
    Reset and deactivate the global whylogs logging session.
    """
    global _session
    if _session is not None:
        _session.close()
    config: SessionConfig = load_config()
    if config is None:
        config = SessionConfig(
            "default-project",
            "default-pipeline",
            [WriterConfig(type="local", output_path="output", formats=["all"])],
            MetadataConfig(type="local", output_path="output"),
            False,
        )
    _session = session_from_config(config)


def start_whylabs_session(
    path_to_config: Optional[str] = None,
    report_progress: Optional[bool] = False,
):
    warnings.warn("start_whylabs_session() is deprecated. Please sign up for WhyLabs", DeprecationWarning)
    global _use_whylabs_client
    _use_whylabs_client = True
    return get_or_create_session(path_to_config, report_progress)


def get_or_create_session(path_to_config: Optional[str] = None, report_progress: Optional[bool] = False):
    """
    Retrieve the current active global session.

    If no active session exists, attempt to load config and create a new
    session.

    If an active session exists, return the session without loading new
    config.

    :return: The global active session
    :rtype: Session
    :type path_to_config: str
    """
    global _session
    if _session is not None and _session.is_active():
        _getLogger(__name__).debug("Active session found, ignoring session kwargs")
    else:
        config = load_config(path_to_config)
        if config is None:
            print("WARN: Missing config")

            config = SessionConfig(
                "default-project",
                "default-pipeline",
                [WriterConfig(type="local", output_path="output", formats=["all"])],
                MetadataConfig(type="local", output_path="output", input_path=""),
                False,
            )
        if report_progress is not None:
            config.report_progress = report_progress

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
    return _session.logger()
