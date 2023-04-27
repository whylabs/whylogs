import time
from functools import reduce
from itertools import groupby
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union, cast

import pandas as pd
from faster_fifo import Queue
from whylogs.api.logger.experimental.multi_dataset_logger.multi_dataset_rolling_logger import (
    MultiDatasetRollingLogger,
)
from whylogs.api.logger.experimental.multi_dataset_logger.string_util import encode_strings
from whylogs.api.logger.experimental.multi_dataset_logger.time_util import Schedule
from whylogs.api.logger.experimental.multi_dataset_logger.time_util import TimeGranularity
from whylogs.api.writer import Writer
from whylogs.core.schema import DatasetSchema

from .actor_process import ActorProcess, CloseMessage

from .profile_actor_messages import (
    LogEmbeddingRequestDict,
    LogRequestDict,
    PublishMessage,
    RawLogEmbeddingsMessage,
    RawLogMessage,
    RawPubSubEmbeddingMessage,
    RawPubSubMessage,
    determine_dataset_timestamp,
    get_columns,
    log_dict_to_data_frame,
    log_dict_to_embedding_matrix,
    reduce_embeddings_request,
    reduce_log_requests,
)

MessageType = Union[PublishMessage, RawLogMessage, RawLogEmbeddingsMessage, RawPubSubMessage, RawPubSubEmbeddingMessage]

DataTypes = Union[str, int, float, bool, List[float], List[int], List[str]]

DictType = TypeVar("DictType", bound="Union[LogRequestDict, LogEmbeddingRequestDict]")
Loggable = Union[pd.DataFrame, Dict[str, Any]]


class ProcessLogger(ActorProcess[MessageType]):
    def __init__(
        self,
        queue: Queue,
        aggregate_by: TimeGranularity = TimeGranularity.Hour,
        write_schedule: Optional[Schedule] = Schedule(cadence=TimeGranularity.Minute, interval=10),
        schema: Optional[DatasetSchema] = None,
        writers: List[Writer] = [],
    ) -> None:
        super().__init__(queue)
        self.loggers: Dict[str, MultiDatasetRollingLogger] = {}
        self.writers = writers
        self.write_schedule = write_schedule
        self.schema = schema
        self.aggregate_by = aggregate_by

    def _create_logger(self, dataset_id: str) -> MultiDatasetRollingLogger:
        logger = MultiDatasetRollingLogger(
            aggregate_by=self.aggregate_by,
            writers=self.writers,
            schema=self.schema,
            write_schedule=self.write_schedule,
        )

        self._logger.info(f"Created logger for {dataset_id}")
        return logger

    def _get_logger(self, dataset_id: str) -> MultiDatasetRollingLogger:
        if dataset_id not in self.loggers:
            self.loggers[dataset_id] = self._create_logger(dataset_id)
        return self.loggers[dataset_id]

    def process_batch(self, batch: List[MessageType], batch_type: Type) -> None:
        if batch_type == PublishMessage:
            self.process_publish_message(cast(List[PublishMessage], batch))
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
        else:
            raise Exception(f"Unknown message type {batch_type}")

    def process_close_message(self, messages: List[CloseMessage]) -> None:
        self._logger.info("Running pre shutdown operations")
        self._logger.info(f"Closing down {len(self.loggers)} loggers")
        for datasetId, logger in self.loggers.items():
            self._logger.info(f"Closing whylogs logger for {datasetId}")
            logger.close()

    def process_pubsub(self, messages: List[RawPubSubMessage]) -> None:
        self._logger.info("Processing pubsub message")
        msgs = [msg["log_request"] for msg in [it.to_pubsub_message() for it in messages] if msg is not None]
        self.process_log_dicts(msgs)

    def process_pubsub_embedding(self, messages: List[RawPubSubEmbeddingMessage]) -> None:
        self._logger.info("Processing pubsub embedding message")
        pubsub = [
            msg["log_embedding_request"]
            for msg in [it.to_pubsub_embedding_message() for it in messages]
            if msg is not None
        ]
        self.process_log_embeddings_dicts(pubsub)

    def process_raw_log_dicts(self, messages: List[RawLogMessage]) -> None:
        self._logger.info("Processing raw log request message")
        log_dicts = [msg for msg in [m.to_log_request_dict() for m in messages] if msg is not None]
        self.process_log_dicts(log_dicts)

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
        pre_processor: Callable[[DictType], Tuple[Loggable, int]],
    ) -> None:
        for dataset_id, group in groupby(dicts, lambda it: it["datasetId"]):
            for dataset_timestamp, ts_grouped in groupby(
                group, lambda it: determine_dataset_timestamp(self.aggregate_by, it)
            ):
                for n, sub_group in groupby(ts_grouped, lambda it: encode_strings(get_columns(it))):
                    self._logger.info(
                        f"Logging data for ts {dataset_timestamp} in dataset {dataset_id} for column set {n}"
                    )
                    giga_message = reduce(reducer, sub_group)
                    loggable, row_count = pre_processor(giga_message)
                    start = time.perf_counter()
                    logger = self._get_logger(dataset_id)
                    logger.log(loggable, timestamp_ms=dataset_timestamp, sync=True)
                    self._logger.debug(f"Took {time.perf_counter() - start}s to log {row_count} rows")

    def process_publish_message(self, messages: Optional[List[PublishMessage]] = None) -> None:
        if not self.loggers:
            self._logger.debug("No profiles to publish")
            return

        self._logger.debug("Force publishing profiles")
        for dataset_id, logger in self.loggers.items():
            self._logger.info(f"Force rolling dataset {dataset_id}")
            logger.flush()
