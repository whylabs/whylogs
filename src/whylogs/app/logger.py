import datetime
import traceback
from typing import List, Optional, Dict

import pandas as pd
import typing

from whylogs.app.writers import Writer
from whylogs.core import DatasetProfile


class Logger:
    """
    Logger class that handles individual dataset logging.

    """

    def __init__(
        self,
        dataset_name: str,
        writers=List[Writer],
        dataset_timestamp: Optional[datetime.datetime] = None,
        session_timestamp: Optional[datetime.datetime] = None,
        tags: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, str]] = None,
        verbose: bool = False,
    ):
        """
        Parameters
        ----------
        dataset_name name of the dataset to log
        writers list of writers to use
        dataset_timestamp Optional. The timestamp of the dataset in UTC
        session_timestamp Optional. The session timestamp in UTC. Default is the current session's timestamp
        tags a dict of key-value. Used for grouping
        metadata a dict of key-value. Used for additional metadata
        verbose enable verbose logging or not
        """
        if session_timestamp is None:
            session_timestamp = datetime.datetime.now(datetime.timezone.utc)
        self.dataset_name = dataset_name
        self.writers = writers
        self.verbose = verbose
        self._profile = DatasetProfile(
            dataset_name,
            dataset_timestamp=dataset_timestamp,
            session_timestamp=session_timestamp,
            tags=tags,
            metadata=metadata,
        )
        self._active = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except Exception as e:
            traceback.print_exception(e)
            pass

    def flush(self):
        if not self._active:
            print("WARNING: attempting to flush a closed logger")
            return

        for writer in self.writers:
            writer.write(self._profile)

    def close(self):
        if not self._active:
            print("WARNING: attempting to close a closed logger")
            return

        self.flush()

        self._active = False

    @property
    def profile(self) -> DatasetProfile:
        """
        Returns
        -------
        profile : DatasetProfile
            the underlying DatasetProfile object for the logger.
            Be careful when modifying this object as it might break the logger.
        """
        return self._profile

    def log(self, columns: typing.Union[str, dict], data):
        """
        Add value(s) to tracking statistics for column(s)

        Parameters
        ----------
        columns : str, dict
            Either the name of a column, or a dictionary specifying column
            names and the data (value) for each column
            If a string, `data` must be supplied.  Otherwise, `data` is
            ignored.
        data : object, None
            Value to track.  Specify if `columns` is a string.
        """
        self._profile.track(columns, data)

    def log_dataframe(self, df: pd.DataFrame):
        """
        Generate and log a WhyLogs DatasetProfile from a pandas dataframe

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe to log
        """
        if not self.is_active():
            return
        self._profile.track_dataframe(df)

    def is_active(self):
        return self._active
