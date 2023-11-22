from abc import abstractmethod
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

try:
    import pandas as pd  # type: ignore
except ImportError:
    pd: Any = None  # type: ignore


# TODO add strong typing here. whylogs takes pretty much anything in log() so we don't actually
# know which types are ok upfront. Right now, this ends up making numpy arrays ok, which is true,
# but the logger just so happens to not really throw when something happens to be a numpy array, it
# isn't explicitly supported. The process logger also supports fewer data types than the thread logger
# because it needs the data to be serializable.
Row = Dict[str, Any]
TrackData = Union[pd.DataFrame, Row, List[Row]]

StatusType = TypeVar("StatusType")


class DataLogger(Generic[StatusType]):
    """
    Interface for data loggers. This type has all of the methods that people would need for actually
    logging data. Its potentially useful to use this as a return type instead of the process/thread logger
    since those have a lot more private methods that aren't useful to the end user.
    """

    @abstractmethod
    def status(self, timeout: Optional[float] = 1.0) -> StatusType:
        """
        Get diagnostic information about the state of the data logger
        """
        raise NotImplementedError()

    @abstractmethod
    def log(
        self,
        data: TrackData,
        timestamp_ms: Optional[int] = None,  # The timestamp that the data happened at
        sync: bool = False,
    ) -> None:
        """
        Log some data.

        Parameters:
            data: The data to log. This can either be a pandas data frame, a row (dictionary of str to str/int/float/etc),
                or a list of rows.
            timestamp_ms: The timestamp of the data. If this isn't supplied then it is assumed to have happened now.
            sync: Whether or not to perform this action synchronously. By default, this is an asynchronous operation.
                You can make this synchronous in order to react to errors. Mostly useful when initially setting up
                logging since the only errors that can be responded to are data format related.
        """
        raise NotImplementedError()

    @abstractmethod
    def flush(self) -> None:
        """
        Flush the internal state, causing everything to be written using the configured writers.
        """
        raise NotImplementedError()

    @abstractmethod
    def close(self) -> None:
        """
        Close the logger, causing all resources to be released.
        """
        raise NotImplementedError()
