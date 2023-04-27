from faster_fifo import Queue

from whylogs.api.logger.experimental.multi_dataset_logger.actor_process import (
    ActorProcess,
)
from whylogs.api.logger.experimental.multi_dataset_logger.multi_dataset_rolling_logger import (
    FlushMessage,
    LoggerMessage,
    MultiDatasetRollingLogger,
    TrackData,
    TrackMessage,
)
from whylogs.api.logger.experimental.multi_dataset_logger.time_util import Schedule, current_time_ms
from whylogs.api.logger.experimental.multi_dataset_logger.time_util import (
    TimeGranularity,
)
from whylogs.api.writer import Writer
from whylogs.core.schema import DatasetSchema
from typing import Optional, Type, List


class ProfileActor(ActorProcess[LoggerMessage]):
    def __init__(
        self,
        queue: Queue,
        aggregate_by: TimeGranularity = TimeGranularity.Hour,
        write_schedule: Optional[Schedule] = Schedule(cadence=TimeGranularity.Minute, interval=10),
        schema: Optional[DatasetSchema] = None,
        writers: List[Writer] = [],
    ) -> None:
        self.writers = writers
        self.aggregate_by = aggregate_by
        self.write_schedule = write_schedule
        self.schema = schema
        # self.multi_logger = MultiDatasetRollingLogger(
            # writers=writers, aggregate_by=aggregate_by, write_schedule=write_schedule, schema=schema
        # )
        super().__init__(queue)

    def after_start(self) -> None:
        import os
        self._logger.info(f"Staring the multilogger thread in pid {os.getpid()}")
        self.multi_logger = MultiDatasetRollingLogger(
            writers=self.writers, aggregate_by=self.aggregate_by, write_schedule=self.write_schedule, schema=self.schema
        )

    def process_batch(self, batch: List[LoggerMessage], batch_type: Type) -> None:
        for message in batch:
            self._logger.info(f"forwarding message to multi logger {self.multi_logger}")
            self.multi_logger.send(message)

    def log(
        self,
        data: TrackData,
        timestamp_ms: Optional[int] = None,  # Not the dataset timestamp, but the timestamp of the data
    ) -> None:
        self.send(TrackMessage(data=data, timestamp_ms=timestamp_ms or current_time_ms(), result=None))


    def flush(self) -> None:
        self.send(FlushMessage())

