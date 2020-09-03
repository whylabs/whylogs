"""
Class and functions for WhyLogs logging
"""
import datetime
from typing import List, Optional

import pandas as pd

from whylogs.app.writers import Writer
from whylogs.core import DatasetProfile


class Logger:
    """
    Class for logging WhyLogs statistics.

    TODO: logger overrides for session config

    Parameters
    ----------
    dataset_name : str
        The name of the dataset. Gets included in the DatasetProfile metadata
        and can be used in generated filenames.
    dataset_timestamp : datetime.datetime
        Timestamp of the data.
    session_timestamp : datetime.datetime
        Timestamp of the logging session
    writers : list
        A list of `Writer` objects which will be used to write the dataset
        profile.
    verbose : bool
        Control output verbosity
    """

    def __init__(
        self,
        dataset_name: str,
        dataset_timestamp: Optional[datetime.datetime] = None,
        session_timestamp: Optional[datetime.datetime] = None,
        writers=List[Writer],
        verbose: bool = False,
    ):
        if session_timestamp is None:
            session_timestamp = datetime.datetime.now(datetime.timezone.utc)
        self.dataset_name = dataset_name
        self.writers = writers
        self.verbose = verbose
        self._profile = DatasetProfile(
            dataset_name,
            data_timestamp=dataset_timestamp,
            session_timestamp=session_timestamp,
        )
        self._active = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def flush(self):
        """
        Synchronously perform all remaining write tasks
        """
        if not self._active:
            print("WARNING: attempting to flush a closed logger")
            return

        for writer in self.writers:
            writer.write(self._profile)

    def close(self):
        """
        Flush and close out the logger.
        """
        if not self._active:
            print("WARNING: attempting to close a closed logger")
            return

        self.flush()

        self._active = False

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
        """
        Return the boolean state of the logger
        """
        return self._active
