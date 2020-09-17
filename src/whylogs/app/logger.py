"""
Class and functions for WhyLogs logging
"""
import datetime
import typing
from typing import List, Optional

import pandas as pd
from pandas._typing import FilePathOrBuffer

from whylogs.app.writers import Writer
from whylogs.core import DatasetProfile


class Logger:
    """
    Class for logging WhyLogs statistics.


    Parameters
    ----------
    dataset_name : str
        The name of the dataset. Gets included in the DatasetProfile metadata
        and can be used in generated filenames.
    dataset_timestamp : datetime.datetime
        Timestamp of the data.
    session_timestamp : datetime.datetime
        Timestamp of the logging session
    tags: dict
        A mapping of (key, value). Used to aggregate the data upstream.
    metadata:
        A mapping of (key, value). Useful for debugging (i.e. associate the original data with individual machines)
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
        tags: typing.Dict[str, str] = None,
        metadata: typing.Dict[str, str] = None,
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
            tags=tags,
            metadata=metadata,
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

    def log(
        self,
        features: typing.Dict[str, any] = None,
        feature_name: str = None,
        value: any = None,
    ):
        """
        Logs a collection of features or a single feature (must specify one or the other).

        Parameters
        ----------
        features a dictionary of key->value for multiple features. Each entry represent a single columnar feature
        feature_name name of a single feature. Cannot be specified if 'features' is speecified
        value value of as single feature. Cannot be specified if 'features' is specified

        Returns
        -------

        """
        if not self._active:
            return

        if features is None and feature_name is None:
            return

        if features is not None and feature_name is not None:
            raise ValueError("Cannot specify both features and feature_name")

        if features is not None:
            self._profile.track(features)
        else:
            self._profile.track_datum(feature_name, value)

    def log_csv(self, filepath_or_buffer: FilePathOrBuffer, **kwargs):
        """
        Log a CSV file. This supports the same parameters as :func`pandas.red_csv<pandas.read_csv>` function.

        Parameters
        ----------
        filepath_or_buffer : Union[str, Path, IO[AnyStr]]
            A path to the CSV file or an IO Stream for CSV data
        """
        if not self._active:
            return

        df = pd.read_csv(filepath_or_buffer, **kwargs)
        self._profile.track_dataframe(df)

    def log_dataframe(self, df: pd.DataFrame):
        """
        Generate and log a WhyLogs DatasetProfile from a pandas dataframe

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe to log
        """
        if not self._active:
            return
        self._profile.track_dataframe(df)

    def is_active(self):
        """
        Return the boolean state of the logger
        """
        return self._active
