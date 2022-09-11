import atexit
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from whylogs.api.logger.result_set import ProfileResultSet, ResultSet
from whylogs.api.logger.segment_processing import segment_processing
from whylogs.api.writer import Writer, Writers
from whylogs.core import DatasetProfile, DatasetSchema
from whylogs.core.errors import LoggingError
from whylogs.core.stubs import pd

logger = logging.getLogger(__name__)


class BasicCache(object):
    _cache: Dict[DatasetSchema, DatasetProfile] = {}

    def get(self, schema: DatasetSchema) -> DatasetProfile:
        candidate = self._cache.get(schema)
        if candidate is None:
            candidate = DatasetProfile(schema=schema)
            self._cache[schema] = candidate
        return candidate


class Logger(ABC):
    def __init__(self, schema: Optional[DatasetSchema] = None):
        self._is_closed = False
        self._schema = schema
        self._writers: List[Writer] = []
        atexit.register(self.close)

    def check_writer(self, _: Writer) -> None:
        """Checks if a writer is configured correctly for this class"""

    def append_writer(self, name: Optional[str] = None, *, writer: Optional[Writer] = None, **kwargs: Any) -> None:
        if name is None and writer is None:
            raise ValueError("Must specify either the writer name or a Writer object")
        if name is not None and writer is not None:
            raise ValueError("Cannot specify name and writer at the same time")
        if name is not None:
            writer = Writers.get(name, **kwargs)

        assert writer is not None
        self.check_writer(writer)
        self._writers.append(writer)

    @abstractmethod
    def _get_matching_profiles(
        self,
        obj: Any = None,
        *,
        pandas: Optional[pd.DataFrame] = None,
        row: Optional[Dict[str, Any]] = None,
    ) -> List[DatasetProfile]:
        pass

    def log(
        self,
        obj: Any = None,
        *,
        pandas: Optional[pd.DataFrame] = None,
        row: Optional[Dict[str, Any]] = None,
    ) -> ResultSet:
        if self._is_closed:
            raise LoggingError("Cannot log to a closed logger")
        if obj is None and pandas is None and row is None:
            # TODO: check for shell environment and emit more verbose error string to let user know how to correct.
            raise LoggingError("log() was called without passing in any input!")

        # If segments are defined use segment_processing to return a SegmentedResultSet
        if self._schema and self._schema.segments:
            return segment_processing(self._schema, obj, pandas, row)

        profiles = self._get_matching_profiles(obj, pandas=pandas, row=row)

        for prof in profiles:
            prof.track(obj, pandas=pandas, row=row)

        return ProfileResultSet(profiles[0])

    @abstractmethod
    def close(self) -> None:
        self._is_closed = True

    def __enter__(self) -> "Logger":
        return self

    def __exit__(self, exception_type: Any, exception_value: Any, traceback: Any) -> None:
        if not self._is_closed:
            self.close()
