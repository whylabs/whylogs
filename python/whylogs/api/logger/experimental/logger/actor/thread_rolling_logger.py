import os
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Type, Union, cast

from dateutil import tz

from whylogs.api.logger.experimental.logger.actor.actor import CloseMessage, QueueConfig
from whylogs.api.logger.experimental.logger.actor.data_logger import (
    DataLogger,
    TrackData,
)
from whylogs.api.logger.experimental.logger.actor.future_util import wait_result
from whylogs.api.logger.experimental.logger.actor.thread_actor import ThreadActor
from whylogs.api.logger.experimental.logger.actor.time_util import (
    FunctionTimer,
    Schedule,
    TimeGranularity,
    current_time_ms,
    truncate_time_ms,
)
from whylogs.api.logger.result_set import ProfileResultSet, ResultSet
from whylogs.api.logger.segment_cache import SegmentCache
from whylogs.api.logger.segment_processing import segment_processing  # type: ignore
from whylogs.api.store import ProfileStore
from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.core import DatasetProfile, DatasetProfileView, DatasetSchema
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView

try:
    import pandas as pd  # type: ignore
except ImportError:
    pd: Any = None  # type: ignore


Row = Dict[str, Any]


class DatasetProfileContainer:
    """
    A container that abstracts over different types of profiles.

    This does the work of deciding how to track data and how to create profiles given a DatasetSchema.
    This can only be used to manage a single entity for a given time. For example, this can represent
    a normal DatasetProfile or segment that has a given dataset timestamp.
    """

    _target: Union[DatasetProfile, SegmentCache]

    def __init__(self, dataset_timestamp: int, schema: Optional[DatasetSchema]) -> None:
        self._schema: Optional[DatasetSchema] = schema
        self._active = True
        self._dataset_timestamp = datetime.fromtimestamp(dataset_timestamp / 1000.0, tz=tz.tzutc())
        if self.has_segments() and schema is not None:  # Need the duplicate None check for type safety
            self._target = SegmentCache(schema=schema)
        else:
            self._target = DatasetProfile(dataset_timestamp=self._dataset_timestamp, schema=schema)

    def has_segments(self) -> bool:
        return self._schema is not None and bool(self._schema.segments)

    def _track_segments(self, data: TrackData) -> None:
        if self._schema is None:
            raise Exception("Schema missing in logger while using segments")

        if not isinstance(self._target, SegmentCache):
            raise Exception("Segment cache missing in logger while using segments")

        if self._schema:
            if isinstance(data, List):
                input_data = [self._schema._run_udfs(pandas=None, row=it)[1] for it in data]  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]
            else:
                df = data if isinstance(data, pd.DataFrame) else None
                row = data if isinstance(data, dict) else None
                df, row = self._schema._run_udfs(df, row)  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType, reportPrivateUsage]
                input_data: TrackData = cast(TrackData, df if df is not None else row)
        else:
            input_data = data

        if isinstance(input_data, List):
            for row in input_data:
                segment_processing(self._schema, row=row, segment_cache=self._target)
        else:
            segment_processing(self._schema, input_data, segment_cache=self._target)

    def _track_profile(self, data: TrackData) -> None:
        if not isinstance(self._target, DatasetProfile):
            raise Exception("Dataset profile missing in logger")

        if isinstance(data, List):
            for row in data:
                self._target.track(row=row)  # type: ignore
        else:
            self._target.track(data)  # type: ignore

    def track(self, data: TrackData) -> None:
        """
        Track data against the contained profile or segment.
        """
        if not self._active:
            # Should never happen
            raise Exception("Profile container no longer active.")

        if self.has_segments():
            self._track_segments(data)
        else:
            self._track_profile(data)

    def to_result_set(self) -> ResultSet:
        """
        Get the ResultSet of the contained profile/segment.

        This doesn't have any side effects. It generates a ResultSet of whatever
        is inside when this is called.
        """
        try:
            if isinstance(self._target, SegmentCache):
                return self._target.flush(dataset_timestamp=self._dataset_timestamp)
            else:
                return ProfileResultSet(self._target)
        finally:
            self._active = False

    def to_views(self) -> List[DatasetProfileView]:
        if isinstance(self._target, SegmentCache):
            result_set = self._target.get_result_set(dataset_timestamp=self._dataset_timestamp)
            segments = result_set.segments() or []
            return [it for it in [result_set.view(segment) for segment in segments] if it is not None]
        else:
            return [self._target.view()]

    def to_serialized_views(self) -> List[bytes]:
        views: List[bytes] = []
        for view in self.to_views():
            views.append(view.serialize())
        return views


@dataclass
class TrackMessage:
    """
    Send some data to be tracked.

    Attributes:
        data: The data to be tracked.
        timestamp_ms: The time in milliseconds when the data occurred.
        result: an optional Future that is fulfilled when the track has completed. It will either
            be a success (None) or a failure (Exception).
    """

    data: TrackData
    timestamp_ms: int
    result: Optional["Future[None]"]


@dataclass
class FlushMessage:
    """
    Trigger a flush, converting all managed profiles to result sets and attempt to write them if there are writers.
    """


@dataclass
class GetResultsMessage:
    result: "Future[Dict[int, List[DatasetProfileView]]]"


@dataclass
class LoggerStatus:
    """
    Various status metrics.

    This returns various metadata about the current state. Useful for logging, testing, and debugging.

    Attributes:
        dataset_timestamps: The amount of dataset timestamps being managed. Each of these will map
            to either a profile or a segment.
        dataset_profiles: The amount of dataset profiles being managed. One of these is created for
            each time period that the logger is configured to manage. For example, if the logger is configured
            to aggregate by hour and TrackMessages come in for two hours, then there will be two of these.
        segment_caches: Same as dataset_profiles, but for segments.
        writers: Amount of writers that the logger is configured to have.
        pending_writables: The amount of items that have been flushed but have not yet been written.
    """

    dataset_timestamps: int
    dataset_profiles: int
    segment_caches: int
    writers: int
    pending_writables: int
    pending_views: List[bytes]
    views: List[bytes]


@dataclass
class StatusMessage:
    """
    Get various status metrics.
    """

    result: "Future[LoggerStatus]"


@dataclass
class PendingWritable:
    attempts: int
    writable: Writable


def _extract_profile_view_bytes(pending: PendingWritable) -> Optional[bytes]:
    if isinstance(pending.writable, DatasetProfile):
        return pending.writable.view().serialize()
    elif isinstance(pending.writable, DatasetProfileView):
        return pending.writable.serialize()
    elif isinstance(pending.writable, SegmentedDatasetProfileView):
        return pending.writable.profile_view.serialize()
    else:
        return None


LoggerMessage = Union[TrackMessage, FlushMessage, StatusMessage, GetResultsMessage, CloseMessage]


class ThreadRollingLogger(ThreadActor[LoggerMessage], DataLogger[LoggerStatus]):
    """
    A logger that manages profiles and segments for various dataset timestamps.

    This logger manages a map of dataset timestamp to dataset profile/segment and handles proper
    logging to each type. Given a TimeGranularity to aggregate by, for each call to track(), roughly
    the following will happen:

        - The timestamp_ms will be truncated to the start of the day/hour (depending on aggregate_by). This
            is the dataset timestamp.
        - That dataset timestamp is used as the key to either create a dataset profile/segment, or to add
            the current data to.

    The logger also periodically attempts to write out the internal state according to the write_schedule. It
    will attempt to write three times before considering a result set unwritable and dropping it. o

    The logger is associated with one or no dataset schema as well. That will determine if the logger creates
    normal profiles or segments internally, among other things.
    """

    def __init__(
        self,
        aggregate_by: TimeGranularity = TimeGranularity.Hour,
        write_schedule: Optional[Schedule] = Schedule(cadence=TimeGranularity.Minute, interval=10),
        schema: Optional[DatasetSchema] = None,
        writers: List[Writer] = [],
        current_time_fn: Optional[Callable[[], int]] = None,
        queue_config: QueueConfig = QueueConfig(),
    ) -> None:
        super().__init__(queue_config=queue_config)
        self._aggregate_by = aggregate_by
        self.current_time_ms = current_time_fn or current_time_ms
        self._cache: Dict[int, DatasetProfileContainer] = {}
        self._timer: Optional[FunctionTimer] = None
        self._writers: Dict[Writer, List[PendingWritable]] = {}
        for writer in writers:
            self._writers[writer] = []
        self._schema: Optional[DatasetSchema] = schema
        self._store_list: List[ProfileStore] = []

        if write_schedule is not None:
            if write_schedule.cadence == TimeGranularity.Second and write_schedule.interval <= 300:
                raise Exception("Minimum write schedule is five minutes.")

            if write_schedule.cadence == TimeGranularity.Minute and write_schedule.interval < 5:
                raise Exception("Minimum write schedule is five minutes.")

            self._timer = FunctionTimer(write_schedule, self.flush)
        else:
            self._logger.warning("No write schedule defined for logger. Profiles will only be written after calls to flush().")

        self._logger.debug(f"Created thread logger, pid {os.getpid()}")

    def process_batch(self, batch: List[LoggerMessage], batch_type: Type[LoggerMessage]) -> None:
        if batch_type == TrackMessage:
            self._process_track_messages(cast(List[TrackMessage], batch))
        elif batch_type == FlushMessage:
            self._process_flush_messages(cast(List[FlushMessage], batch))
        elif batch_type == CloseMessage:
            self._process_close_messages(cast(List[CloseMessage], batch))
        elif batch_type == StatusMessage:
            self._process_status_messages(cast(List[StatusMessage], batch))
        elif batch_type == GetResultsMessage:
            self._process_get_results_messages(cast(List[GetResultsMessage], batch))
        else:
            # Safe guard for forgetting to handle a message in development
            raise Exception(f"Don't know how to handle message {batch_type}")

    def _process_get_results_messages(self, messages: List[GetResultsMessage]) -> None:
        for message in messages:
            self._process_get_results_message(message)

    def _process_get_results_message(self, message: GetResultsMessage) -> None:
        items: Dict[int, List[DatasetProfileView]] = {}
        for dataset_timestamp, container in self._cache.items():
            self._logger.debug(f"Generating views for dataset timestamp {dataset_timestamp}")
            items[dataset_timestamp] = container.to_views()

        message.result.set_result(items)

    def _process_status_messages(self, messages: List[StatusMessage]) -> None:
        for message in messages:
            self._process_status_message(message)

    def _process_status_message(self, message: StatusMessage) -> None:
        profiles = 0
        segment_caches = 0
        views: List[bytes] = []
        for container in self._cache.values():
            if container.has_segments():
                segment_caches += 1
            else:
                profiles += 1

            views.extend(container.to_serialized_views())

        writers = 0
        writables = 0
        pending_views: List[bytes] = []
        for stuff in self._writers.values():
            writers += 1
            writables += len(stuff)
            for pending in stuff:
                view = _extract_profile_view_bytes(pending)
                if view is not None:
                    pending_views.append(view)

        status = LoggerStatus(
            dataset_timestamps=len(self._cache),
            dataset_profiles=profiles,
            segment_caches=segment_caches,
            writers=writers,
            pending_writables=writables,
            pending_views=pending_views,
            views=views,
        )
        message.result.set_result(status)

    def _process_close_messages(self, messages: List[CloseMessage]) -> None:
        for message in messages:
            self._process_close_message(message)

    def _process_close_message(self, message: CloseMessage) -> None:
        # Force wait for all writers to handle their pending items
        self._process_flush_message(FlushMessage())
        while self._has_pending():
            self._process_flush_message(FlushMessage())

    def _has_pending(self) -> bool:
        has_pending = False
        for pending in self._writers.values():
            has_pending = len(pending) > 0
        return has_pending

    def _process_track_messages(self, messages: List[TrackMessage]) -> None:
        for message in messages:
            self._process_track_message(message)

    def _process_flush_messages(self, messages: List[FlushMessage]) -> None:
        for message in messages:
            self._process_flush_message(message)

    def _process_flush_message(self, message: FlushMessage) -> None:
        for dataset_timestamp, container in self._cache.items():
            self._logger.debug(f"Generating result set for dataset timestamp {dataset_timestamp}")

            result_set = container.to_result_set()
            for writable in result_set.get_writables() or []:
                for pending in self._writers.values():
                    pending.append(PendingWritable(attempts=0, writable=writable))

        self._cache = {}
        self._write_pending()

    def _write_pending(self) -> None:
        new_state: Dict[Writer, List[PendingWritable]] = {}
        for writer, pending in self._writers.items():
            failures: List[PendingWritable] = []
            self._logger.info(f"Writing out result set with {type(writer).__name__}")
            for p in pending:
                self._logger.debug(f"Writing {p.attempts} attempt")
                failed = False
                try:
                    success, msg = writer.write(p.writable)

                    if not success:
                        self._logger.error(f"Couldn't write profile: {msg}")
                        failed = True
                except Exception as e:
                    self._logger.exception(e)
                    failed = True

                if failed:
                    p.attempts += 1
                    if p.attempts < 3:
                        failures.append(p)
                    else:
                        self._logger.info(f"Writing failed too many times ({p.attempts}) for {type(writer).__name__}")
            new_state[writer] = failures
        self._writers = new_state

    def _get_profile_container(self, dataset_timestamp: int) -> DatasetProfileContainer:
        if dataset_timestamp not in self._cache:
            self._cache[dataset_timestamp] = DatasetProfileContainer(dataset_timestamp, schema=self._schema)

        return self._cache[dataset_timestamp]

    def _process_track_message(self, message: TrackMessage) -> None:
        try:
            timestamp_ms = message.timestamp_ms
            data = message.data

            ts = timestamp_ms or self.current_time_ms()
            dataset_timestamp = truncate_time_ms(ts, self._aggregate_by)
            profile_container = self._get_profile_container(dataset_timestamp)
            # TODO consider porting out the aggregation logic that the process rolling logger uses to batch up
            # data and minimize the amount of track calls. It makes a really big difference.
            profile_container.track(data)
            if message.result is not None:
                message.result.set_result(None)
        except Exception as e:
            if message.result is not None:
                message.result.set_exception(e)

    def status(self, timeout: Optional[float] = None) -> LoggerStatus:
        """
        Get the status of the logger.
        This is always synchronous.
        """
        result: "Future[LoggerStatus]" = Future()
        self.send(StatusMessage(result))
        return wait_result(result, timeout=timeout)

    def _validate_data(self, data: TrackData) -> None:
        if not isinstance(data, pd.DataFrame) and not isinstance(data, list) and not isinstance(data, dict):
            raise Exception(f"Unsupported data type {type(data)}")

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
        self._validate_data(data)

        result: Optional["Future[None]"] = Future() if sync else None
        self.send(
            TrackMessage(
                data=data,
                timestamp_ms=timestamp_ms or self.current_time_ms(),
                result=result,
            )
        )
        if result is not None:
            self._logger.debug("Waiting for track to complete")
            wait_result(result)

    def flush(self) -> None:
        """
        Flush the internal state, causing everything to be written using the configured writers.
        """
        self.send(FlushMessage())

    def get_profile_views(self) -> Dict[int, List[DatasetProfileView]]:
        """
        Get all of the profile views for each dataset timestamp being maintained.
        """
        result: "Future[Dict[int, List[DatasetProfileView]]]" = Future()
        self.send(GetResultsMessage(result))
        return wait_result(result)

    def close(self) -> None:
        if self._timer is not None:
            self._timer.stop()
        super().close()
