from typing import Optional, List

import pandas as pd

from whylabs.logs.app.writer import Writer
from whylabs.logs.core import datasetprofile


class Logger:
    """
    Class for logging WhyLogs statistics.

    TODO: logger overrides for session config

    Parameters
    ----------

    """

    def __init__(self,
                 dataset_name: str,
                 datetime_column: Optional[str] = None,
                 datetime_format: Optional[str] = None,
                 writers=List[Writer],
                 verbose: bool = False):
        self.dataset_name = dataset_name
        self.datetime_column = datetime_column
        self.datetime_format = datetime_format
        self.writers = writers
        self.verbose = verbose
        self._profile = datasetprofile.DatasetProfile(dataset_name)
        self._active = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def flush(self):
        for writer in self.writers:
            writer.write(self._profile)

    def close(self):
        if not self._active:
            print('WARNING: attempting to close a closed logger')
            return

        self._active = False

    def log_dataframe(self, df: pd.DataFrame):
        """
        Generate and log a WhyLogs DatasetProfile from a pandas dataframe

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe to log
        name : str
            Name of the dataset, e.g. 'training.data'
        """
        if not self.is_active():
            return
        self._profile.track_dataframe(df)

    def is_active(self):
        return self._active
