import logging
import multiprocessing as mp
import os
import threading as th
import time
from abc import abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass, field
from functools import reduce
from itertools import groupby
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    NoReturn,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from whylogs.api.whylabs.session.config import INIT_DOCS
from whylogs.api.whylabs.session.session_manager import default_init

try:
    import orjson  # type: ignore
except ImportError:
    from whylogs.api.logger.experimental.logger.actor.proc_error_message import (
        proc_error_message,
    )

    raise ImportError(proc_error_message)

# Pandas/numpy required in the proc extra that this class requires
import pandas as pd

from whylogs.api.logger.experimental.logger.actor.actor import CloseMessage, QueueConfig
from whylogs.api.logger.experimental.logger.actor.data_logger import (
    DataLogger,
    TrackData,
)
from whylogs.api.logger.experimental.logger.actor.future_util import wait_result_while
from whylogs.api.logger.experimental.logger.actor.process_actor import (
    ProcessActor,
    QueueType,
)
from whylogs.api.logger.experimental.logger.actor.process_rolling_logger_messages import (
    DataDict,
    FlushMessage,
    LogEmbeddingRequestDict,
    LogMessage,
    LogRequestDict,
    ProcessLoggerStatus,
    ProcessLoggerStatusMessage,
    RawLogEmbeddingsMessage,
    RawLogMessage,
    RawPubSubEmbeddingMessage,
    RawPubSubMessage,
    data_dict_from_pandas,
    determine_dataset_timestamp,
    get_columns,
    log_dict_to_data_frame,
    log_dict_to_embedding_matrix,
    reduce_embeddings_request,
    reduce_log_requests,
)
from whylogs.api.logger.experimental.logger.actor.string_util import encode_strings
from whylogs.api.logger.experimental.logger.actor.thread_rolling_logger import (
    LoggerStatus,
    StatusMessage,
    ThreadRollingLogger,
)
from whylogs.api.logger.experimental.logger.actor.time_util import (
    Schedule,
    TimeGranularity,
    current_time_ms,
)
from whylogs.api.writer import Writer, Writers
from whylogs.core.schema import DatasetSchema

DataTypes = Union[str, int, float, bool, List[float], List[int], List[str]]

DictType = TypeVar("DictType", LogRequestDict, LogEmbeddingRequestDict)
# Loggable = Union[pd.DataFrame, Dict[str, Any], "np.ndarray[Any, Any]"]


class WriterFactory:
    @abstractmethod
    def create_writers(self, dataset_id: str) -> List[Writer]:
        raise NotImplementedError()


class WhyLabsWriterFactory(WriterFactory):
    def create_writers(self, dataset_id: str) -> List[Writer]:
        return [
            Writers.get(
                "whylabs",
                dataset_id=dataset_id,
            )
        ]


@dataclass
class LoggerOptions:
    aggregate_by: TimeGranularity = TimeGranularity.Hour
    write_schedule: Optional[Schedule] = field(
        default_factory=lambda: Schedule(cadence=TimeGranularity.Minute, interval=5)
    )
    schema: Optional[DatasetSchema] = None
    sync_enabled: bool = False
    current_time_fn: Optional[Callable[[], int]] = None
    queue_config: QueueConfig = QueueConfig()
    thread_queue_config: QueueConfig = QueueConfig()
    writer_factory: WriterFactory = field(default_factory=WhyLabsWriterFactory)
    queue_type: QueueType = QueueType.FASTER_FIFO


class LoggerFactory:
    @abstractmethod
    def create_logger(self, dataset_id: str, options: LoggerOptions) -> ThreadRollingLogger:
        raise NotImplementedError()


class ThreadLoggerFactory(LoggerFactory):
    def create_logger(self, dataset_id: str, options: LoggerOptions) -> ThreadRollingLogger:
        logger = ThreadRollingLogger(
            aggregate_by=options.aggregate_by,
            writers=options.writer_factory.create_writers(dataset_id),
            schema=options.schema,
            write_schedule=options.write_schedule,
            current_time_fn=options.current_time_fn,
            queue_config=options.thread_queue_config,
        )

        return logger


BuiltinMessageTypes = Union[
    FlushMessage,
    RawLogMessage,
    RawLogEmbeddingsMessage,
    RawPubSubMessage,
    RawPubSubEmbeddingMessage,
    LogMessage,
    CloseMessage,
    ProcessLoggerStatusMessage,
]

AdditionalMessages = TypeVar("AdditionalMessages")


class BaseProcessRollingLogger(
    ProcessActor[Union[AdditionalMessages, BuiltinMessageTypes]],
    DataLogger[Dict[str, ProcessLoggerStatus]],
    Generic[AdditionalMessages],
):
    """
    Log data asynchronously using a separate process.

    The ProcessRollingLogger is a rolling logger that manages a separate process to do the actual logging. This means
    it logs data over time and periodically uploads it in the background, using a separate process so that it doesn't
    block the main one.

    ```python
    logger = ProcessRollingLogger(
        aggregate_by=TimeGranularity.Day,
        write_schedule=Schedule(cadence=TimeGranularity.Minute, interval=5),
    )

    logger.start()

    logger.log(data_frame)
    ```

    This class mostly wraps and manages several ThreadRollingLoggers that do the real logging with whylogs.

    MAC USERS: You'll run into issues running this on Python>=3.8 because Python will use spawn instead of fork.
    You should be able to get around it by setting the environment variable OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
    in the environment that the process logger runs in, but you can't set it in Python (no using os.environ).

    Most of the arguments that are passed to the underlying loggers are considered the default options for those
    loggers. If you supply a logger_factory then you can override the options for each dataset id's logger.

    Args:
        aggregate_by: The time granularity to aggregate data by. This determines how the time bucketing is done. For
            the Hour type, the logger will end up pooling data into profiles by the hour.
        write_schedule: The schedule to use for writing data. This is used to determine when to upload data.
        schema: The DatasetSchema to use for whylogs under the hood.
        sync_enabled: Whether to enable synchronous logging. If this is enabled then you can pass log(sync=True) to the
            log call. Without this you can't use the sync flag.
        queue_config: Let's you change various polling and timeout parameters.
        thread_queue_config: Same as queue_config, but for the wrapped ThreadRollingLoggers.
        writer_factory: The writer factory to use for creating writers.
        queue_type: The type of queue to to manage multiprocessing. By default, faster_fifo is used because it's
            a lot faster than the default multiprocessing queue, but you can use the built in mp.Queue by setting
            this to QueueType.MP.
    """

    def __init__(
        self,
        aggregate_by: TimeGranularity = TimeGranularity.Day,
        write_schedule: Optional[Schedule] = Schedule(cadence=TimeGranularity.Minute, interval=5),
        schema: Optional[DatasetSchema] = None,
        sync_enabled: bool = False,
        current_time_fn: Optional[Callable[[], int]] = None,
        queue_config: QueueConfig = QueueConfig(),
        thread_queue_config: QueueConfig = QueueConfig(),
        writer_factory: WriterFactory = WhyLabsWriterFactory(),
        queue_type: QueueType = QueueType.FASTER_FIFO,
        logger_factory: LoggerFactory = ThreadLoggerFactory(),
    ) -> None:
        super().__init__(queue_config=queue_config, queue_type=queue_type)
        self._logger_options = LoggerOptions(
            aggregate_by=aggregate_by,
            write_schedule=write_schedule,
            schema=schema,
            sync_enabled=sync_enabled,
            current_time_fn=current_time_fn,
            queue_config=queue_config,
            thread_queue_config=thread_queue_config,
            writer_factory=writer_factory,
        )
        self._logger_factory = logger_factory

        self._sync_enabled = sync_enabled
        self._thread_queue_config = thread_queue_config
        self._writer_factory = writer_factory
        self.current_time_ms = current_time_fn or current_time_ms
        self.loggers: Dict[str, ThreadRollingLogger] = {}
        self.schema = schema
        self._pipe_signaler: Optional[PipeSignaler] = PipeSignaler() if sync_enabled else None
        self._session = default_init()

    def _create_logger(self, dataset_id: str) -> ThreadRollingLogger:
        return self._logger_factory.create_logger(dataset_id, self._logger_options)

    def _get_logger(self, dataset_id: str) -> ThreadRollingLogger:
        if dataset_id not in self.loggers:
            self.loggers[dataset_id] = self._create_logger(dataset_id)
        return self.loggers[dataset_id]

    def process_batch(
        self,
        batch: List[Union[AdditionalMessages, BuiltinMessageTypes]],
        batch_type: Type[Union[AdditionalMessages, BuiltinMessageTypes]],
    ) -> None:
        if batch_type == FlushMessage:
            self.process_flush_message(cast(List[FlushMessage], batch))
        elif batch_type == LogMessage:
            self.process_log_messages(cast(List[LogMessage], batch))
        elif batch_type == RawLogMessage:
            self.process_raw_log_dicts(cast(List[RawLogMessage], batch))
        elif batch_type == RawLogEmbeddingsMessage:
            self.process_log_embeddings_messages(cast(List[RawLogEmbeddingsMessage], batch))
        elif batch_type == RawPubSubMessage:
            self.process_pubsub(cast(List[RawPubSubMessage], batch))
        elif batch_type == RawPubSubEmbeddingMessage:
            self.process_pubsub_embedding(cast(List[RawPubSubEmbeddingMessage], batch))
        elif batch_type == CloseMessage:
            self.process_close_message(cast(List[CloseMessage], batch))
        elif batch_type == ProcessLoggerStatusMessage:
            self._process_logger_status_message(cast(List[ProcessLoggerStatusMessage], batch))
        else:
            raise Exception(f"Unknown message type {batch_type}")

    def process_close_message(self, messages: List[CloseMessage]) -> None:
        self._logger.info("Running pre shutdown operations")
        self._logger.info(f"Closing down {len(self.loggers)} loggers")
        for datasetId, logger in self.loggers.items():
            self._logger.info(f"Closing whylogs logger for {datasetId}")
            logger.close()

        if self._pipe_signaler is not None:
            self._pipe_signaler.close_child()

    def process_pubsub(self, messages: List[RawPubSubMessage]) -> None:
        self._logger.info("Processing pubsub message")
        msgs = [msg["log_request"] for msg in [it.to_pubsub_message() for it in messages] if msg is not None]
        self.process_log_dicts(msgs)

    def _process_logger_status_message(self, messages: List[ProcessLoggerStatusMessage]) -> None:
        if self._pipe_signaler is None:
            raise Exception(
                "Can't log synchronously without a pipe signaler. Initialize the process logger with sync_enabled=True."
            )

        futures: List[Tuple[str, "Future[LoggerStatus]"]] = []

        for dataset_id, logger in self.loggers.items():
            future: "Future[LoggerStatus]" = Future()
            logger.send(StatusMessage(result=future))
            futures.append((dataset_id, future))

        statuses: List[ProcessLoggerStatus] = []
        for dataset_id, future in futures:
            try:
                status = ProcessLoggerStatus(dataset_id=dataset_id, status=wait_result_while(future, self.is_alive))
                statuses.append(status)
            except Exception as e:
                for message in messages:
                    self._pipe_signaler.signal((message.id, e, None))

        # Signal all of the status. In practice, there will really only be a single message in messages
        # but we do handle messages in batches so its technically possible to have multiple if the caller
        # is just spamming status requests for some reason.
        status_dict = {status.dataset_id: status for status in statuses}
        for message in messages:
            self._pipe_signaler.signal((message.id, None, status_dict))

    def status(self, timeout: Optional[float] = 1.0) -> Dict[str, ProcessLoggerStatus]:
        """
        Get the internal status of the logger. Used for diangostics and debugging.
        """
        if self._pipe_signaler is None:
            raise Exception(
                "Can't log synchronously without a pipe signaler. Initialize the process logger with sync_enabled=True."
            )

        message = ProcessLoggerStatusMessage()
        future: "Future[Dict[str, ProcessLoggerStatus]]" = Future()
        self._pipe_signaler.register(future, message.id)
        self.send(message)
        return wait_result_while(future, self.is_alive)

    def process_pubsub_embedding(self, messages: List[RawPubSubEmbeddingMessage]) -> None:
        self._logger.info("Processing pubsub embedding message")
        pubsub = [
            msg["log_embedding_request"]
            for msg in [it.to_pubsub_embedding_message() for it in messages]
            if msg is not None
        ]
        self.process_log_embeddings_dicts(pubsub)

    def process_log_messages(self, messages: List[LogMessage]) -> None:
        try:
            self._logger.info("Processing log message")
            log_dicts = [m.log for m in messages]
            self.process_log_dicts(log_dicts)

            for message in messages:
                self._signal(message.id, None)
        except Exception as e:
            self._logger.exception("Error processing log message")
            for message in messages:
                self._signal(message.id, e)

    def _signal(self, message_id: str, error: Optional[Exception] = None) -> None:
        if self._pipe_signaler is not None:
            self._pipe_signaler.signal((message_id, error, None))

    def process_raw_log_dicts(self, messages: List[RawLogMessage]) -> None:
        try:
            self._logger.info("Processing raw log request message")
            log_dicts = [msg for msg in [m.to_log_request_dict() for m in messages] if msg is not None]
            self.process_log_dicts(log_dicts)
            for message in messages:
                self._signal(message.id, None)
        except Exception as e:
            self._logger.exception("Error processing log message")
            for message in messages:
                self._signal(message.id, e)

    def process_log_embeddings_messages(self, messages: List[RawLogEmbeddingsMessage]) -> None:
        self._logger.info("Processing log embeddings messages")
        log_dicts = [msg for msg in [m.to_log_embeddings_request_dict() for m in messages] if msg is not None]
        self.process_log_embeddings_dicts(log_dicts)

    def process_log_embeddings_dicts(self, messages: List[LogEmbeddingRequestDict]) -> None:
        self._logger.info("Processing log embeddings dicts")
        self._process_dicts(messages, reduce_embeddings_request, log_dict_to_embedding_matrix)

    def process_log_dicts(self, messages: List[LogRequestDict]) -> None:
        self._process_dicts(messages, reduce_log_requests, log_dict_to_data_frame)

    def _process_dicts(
        self,
        dicts: List[DictType],
        reducer: Callable[[DictType, DictType], DictType],
        pre_processor: Callable[[DictType], Tuple[TrackData, int]],
    ) -> None:
        for dataset_id, group in groupby(dicts, lambda it: it["datasetId"]):
            for dataset_timestamp, ts_grouped in groupby(
                group, lambda it: determine_dataset_timestamp(self._logger_options.aggregate_by, it)
            ):
                for n, sub_group in groupby(ts_grouped, lambda it: encode_strings(get_columns(it))):
                    self._logger.info(
                        f"Logging data for ts {dataset_timestamp} in dataset {dataset_id} for column set {n}"
                    )
                    giga_message = reduce(reducer, sub_group)
                    loggable, row_count = pre_processor(giga_message)
                    start = time.perf_counter()
                    logger = self._get_logger(dataset_id)
                    # TODO this error looks real. I think the thread logger can't handle numpy arrays currently
                    # TODO unify the Loggable and TrackData types?
                    logger.log(loggable, timestamp_ms=dataset_timestamp, sync=True)
                    self._logger.debug(f"Took {time.perf_counter() - start}s to log {row_count} rows")

    def process_flush_message(self, messages: Optional[List[FlushMessage]] = None) -> None:
        if not self.loggers:
            self._logger.debug("No profiles to publish")
            return

        self._logger.debug("Force publishing profiles")
        for dataset_id, logger in self.loggers.items():
            self._logger.info(f"Force rolling dataset {dataset_id}")
            logger.flush()

    def _create_multiple(self, data: TrackData) -> DataDict:
        if isinstance(data, pd.DataFrame):
            return data_dict_from_pandas(data)
        elif isinstance(data, list):
            # There might be a more performant way of handling lists of rows
            return data_dict_from_pandas(pd.DataFrame(data))
        elif isinstance(data, dict):
            return {
                "columns": list(data.keys()),
                "data": [list(data.values())],
            }
        else:
            raise Exception(f"Unsupported data type {type(data)}")

    def log(
        self,
        data: TrackData,
        timestamp_ms: Optional[int] = None,  # The timestamp that the data happened at
        sync: bool = False,
        dataset_id: Optional[str] = None,
    ) -> None:
        if self.pid is None:
            raise Exception("Logger hasn't been started yet. Call start() first.")

        if not self.is_alive():
            raise Exception("Logger process is no longer alive. It may have been killed.")

        if dataset_id is None:
            dataset_id = self._session.config.get_default_dataset_id()
            if dataset_id is None:
                raise Exception(
                    f"Need to specify a dataset_id when calling log, or set it through why.init(). See {INIT_DOCS}"
                )

        log_request = LogRequestDict(
            datasetId=dataset_id,
            timestamp=timestamp_ms,
            multiple=self._create_multiple(data),
        )

        message = RawLogMessage(request=orjson.dumps(log_request), request_time=self.current_time_ms())
        result: Optional["Future[None]"] = cast("Future[None]", Future()) if sync else None
        if result is not None:
            self._logger.debug(f"Registering result id {message.id} for synchronous logging")
            if self._pipe_signaler is None:
                raise Exception(
                    "Can't log synchronously without a pipe signaler. Initialize the process logger with sync_enabled=True."
                )
            self._pipe_signaler.register(result, message.id)

        self.send(message)

        if result is not None:
            self._logger.debug(f"Waiting on id {message.id}")
            try:
                it = wait_result_while(result, self.is_alive)
                self._logger.debug(f"Result id {message.id} done {it}")
            except TimeoutError as e:
                raise Exception("Logger killed while waiting for result") from e

    def flush(self) -> None:
        """
        Flush the internal state, causing everything to be written using the configured writers.
        """
        self.send(FlushMessage())

    def run(self) -> None:
        self._logger.debug(f"Started process logger with pid {os.getpid()}")
        super().run()

    def start(self) -> None:
        self._logger.debug(f"Starting process logger from pid {os.getpid()}")
        # This is started in the parent process, not in the child process. It must be started
        # before the process itself start right below.
        if self._pipe_signaler is not None:
            self._pipe_signaler.start()
        super().start()

    def close(self) -> None:
        super().close()
        if self._pipe_signaler is not None:
            self._pipe_signaler.close()


class PipeSignaler(th.Thread):
    """
    A thread that listens on a pipe for messages and signals the corresponding futures.

    This class is used in the process logger to enable synchronous logging requests across processes.
    It's essentially a dictionary of futures that are registered by the main process and signaled by the
    child process. A lot of the behavior is implicit because it involves properties of processes, so it's
    worth documenting here.

    - This thread has to be started from the main process, which means it has to be started right before the
        process logger is started (before the os.fork under the hood). It has to be started from the main process
        because the main process will be registering futures on it, and those can't cross the process boundary.
    - The parent and child process each have references to the pipes and they each need to close their references,
        which means close_child has to be called from the child process and close has to be called from the parent.
        Calling close_child in the main processing code will have right effect.
    - The process actor does message batching so multiple ids may be signaled even though a single batch was processed
        because that batch could have contained multiple messages.
    - The signaler uses Events under the hood to know when to stop working. They can be th.Events even though this
        is being used in a multiprocessing environment because nothing the child does can affect them. Keep in mind
        that introducing any behavior on the child side that depends on knowing whether those events are set won't work
        though, they would have to be switched to mp.Events for that.

    This class should really never be used by anyone in most cases. It will just slow down the main process by making
    it wait for logging to complete, but it enables a lot of testing and debugging.
    """

    def __init__(self) -> None:
        super().__init__()
        self.daemon = True
        self._logger = logging.getLogger(__name__)
        self._parent_conn, self._conn = mp.Pipe()
        self.futures: Dict[str, "Future[Any]"] = {}
        self._end_polling = th.Event()
        self._done = th.Event()

    def signal(self, result: Tuple[str, Optional[Exception], Any]) -> None:
        """
        Signal that a message was handled by sending a tuple of (message id, exception, data).
        data and exception can be None.
        This should be called from the child process.
        """
        self._parent_conn.send(result)

    def register(self, future: "Future[Any]", message_id: str) -> None:
        """
        Register a future to be signaled when the message id is received.
        This should be called from the parent process.
        """
        self._logger.debug(f"Received register request for id {message_id}")
        self.futures[message_id] = future

    def _start_poll_conn(self) -> None:
        while not self._end_polling.is_set():
            try:
                if self._conn.poll(timeout=0.1):
                    message_id, exception, data = self._conn.recv()
                    self._logger.debug(f"Received message id {message_id}")
                    future: Optional["Future[Any]"] = self.futures.pop(message_id, None)
                    if future is not None:
                        self._logger.debug(f"Setting result for message id {message_id} {exception}")
                        if exception is None:
                            print(f"Setting result for message id {message_id} {data}")
                            future.set_result(data)
                        else:
                            future.set_exception(exception)

            except EOFError:
                self._logger.exception("Broken pipe")
                break
            except OSError as e:
                self._logger.exception(f"OS Error in ipc pipe. Was the logger closed? {e}")
            except Exception as e:
                self._logger.exception(f"Error in ipc pipe {e}")

        self._done.set()

    def run(self) -> None:
        self._start_poll_conn()

    def close_child(self) -> None:
        """
        Closes the file descriptors from the child process side.
        """
        self._conn.close()
        self._parent_conn.close()

    def close(self) -> None:
        """
        Closes the thread and all resources. This should be
        called from the parent side.
        """
        self._conn.close()
        self._parent_conn.close()

        self._end_polling.set()
        self._done.wait()
        self.join()


class ProcessRollingLogger(BaseProcessRollingLogger[NoReturn]):
    pass
