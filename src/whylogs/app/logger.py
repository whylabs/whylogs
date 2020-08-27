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
