import atexit
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from whylogs.api.logger.result_set import (
    ProfileResultSet,
    ResultSet,
    SegmentedResultSet,
)
from whylogs.api.logger.segment_processing import segment_processing
from whylogs.api.store import ProfileStore
from whylogs.api.writer import Writer, Writers
from whylogs.core import DatasetProfile, DatasetSchema
from whylogs.core.errors import LoggingError
from whylogs.core.input_resolver import _pandas_or_dict
from whylogs.core.metadata import (
    _populate_common_profile_metadata,
    _safe_merge_metadata,
)
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
        self._store_list: List[ProfileStore] = []
        self._segment_cache = None
        self._metadata: Optional[Dict[str, str]] = None

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

    def append_store(self, store: ProfileStore) -> None:
        self._store_list.append(store)

    @abstractmethod
    def _get_matching_profiles(
        self,
        obj: Any = None,
        *,
        pandas: Optional[pd.DataFrame] = None,
        row: Optional[Dict[str, Any]] = None,
        schema: Optional[DatasetSchema] = None,
    ) -> List[DatasetProfile]:
        pass

    def log(
        self,
        obj: Any = None,
        *,
        pandas: Optional[pd.DataFrame] = None,
        row: Optional[Dict[str, Any]] = None,
        schema: Optional[DatasetSchema] = None,
        timestamp_ms: Optional[int] = None,  # Not the dataset timestamp, but the timestamp of the data
        name: Optional[str] = None,
        trace_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
        segment_key_values: Optional[Dict[str, str]] = None,
    ) -> ResultSet:
        """
        Args:
            timestamp_ms: The timestamp of the data being logged. This defaults to now if it isn't provided.
             This is used to determine what the dataset timestamp should be. For an hourly model, the dataset
             timestamp will end up being the start of the hour of the provided timestamp_ms, UTC.
        """
        if self._is_closed:
            raise LoggingError("Cannot log to a closed logger")
        if obj is None and pandas is None and row is None:
            # TODO: check for shell environment and emit more verbose error string to let user know how to correct.
            raise LoggingError("log() was called without passing in any input!")

        if name is not None:
            if self._metadata is None:
                self._metadata = dict()
            self._metadata["name"] = name
        active_schema = schema or self._schema
        if active_schema:
            pandas, row = _pandas_or_dict(obj, pandas, row)
            obj = None
            pandas, row = active_schema._run_udfs(pandas, row)

        # If segments are defined use segment_processing to return a SegmentedResultSet
        if active_schema and active_schema.segments:
            segmented_results: SegmentedResultSet = segment_processing(
                schema=active_schema,
                obj=obj,
                pandas=pandas,
                row=row,
                segment_cache=self._segment_cache,
            )
            # Update the existing segmented_results metadata with the trace_id and other keys if not present
            _populate_common_profile_metadata(segmented_results.metadata, trace_id=trace_id, tags=tags)
            _safe_merge_metadata(default_metadata=segmented_results.metadata, incoming_metadata=active_schema.metadata)
            return segmented_results

        profiles = self._get_matching_profiles(obj, pandas=pandas, row=row, schema=active_schema)

        for prof in profiles:
            prof.track(obj, pandas=pandas, row=row, execute_udfs=False)
            prof._metadata = _populate_common_profile_metadata(prof._metadata, trace_id=trace_id, tags=tags)
            if active_schema:
                _safe_merge_metadata(prof._metadata, active_schema.metadata)

        first_profile = profiles[0]
        if name is not None:
            if first_profile._metadata is None:
                first_profile._metadata = dict()
            first_profile._metadata["name"] = name
        return ProfileResultSet(first_profile)

    def close(self) -> None:
        self._is_closed = True

    def __enter__(self) -> "Logger":
        return self

    def __exit__(self, exception_type: Any, exception_value: Any, traceback: Any) -> None:
        if not self._is_closed:
            self.close()
