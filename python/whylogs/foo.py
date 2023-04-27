from whylogs.api.logger.experimental.multi_dataset_logger.actor_process import start_actor
from faster_fifo import Queue
from whylogs.api.logger.experimental.multi_dataset_logger.multi_dataset_rolling_logger import (
    MultiDatasetRollingLogger,
    TrackData,
)
from whylogs.api.logger.experimental.multi_dataset_logger.process_logger import ProcessLogger
from whylogs.api.logger.experimental.multi_dataset_logger.profile_actor_messages import (
    LogRequest,
    LogMultiple,
    RawLogMessage,
)
from whylogs.api.logger.experimental.multi_dataset_logger.time_util import TimeGranularity, Schedule, current_time_ms
from whylogs.api.writer.whylabs import WhyLabsWriter
import os
import logging


def init_logging():
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # formatter = DefaultFormatter("%(levelprefix)s [%(asctime)s %(name)s] %(message)s", datefmt="%d-%m-%Y-%H:%M:%S")
    # ch.setFormatter(formatter)

    logging.basicConfig(handlers=[ch])
    logging.root.setLevel(logging.DEBUG)


init_logging()

print(f"PID: {os.getpid()}")

os.environ["WHYLABS_DEFAULT_ORG_ID"] = "org-0"
os.environ["WHYLABS_API_KEY"] = "hDQSx9nXzJ.viriMIcZYTnnlAob7wSl7Nbg18v84vISOS5uHYTzFVTNSvEplSlfO"
writer = WhyLabsWriter(
    org_id=os.environ["WHYLABS_DEFAULT_ORG_ID"], api_key=os.environ["WHYLABS_API_KEY"], dataset_id="model-48"
)
schedule = Schedule(cadence=TimeGranularity.Minute, interval=1)

# logger = MultiDatasetRollingLogger(
#     aggregate_by=TimeGranularity.Hour,
#     write_schedule=schedule,
#     writers=[writer],
#     # Can optionally provide a dataset schema here as well.
# )


queue = Queue(1000 * 1000)
logger = ProcessLogger(
    aggregate_by=TimeGranularity.Hour,
    write_schedule=schedule,
    writers=[writer],
    queue=queue
    # Can optionally provide a dataset schema here as well.
)
start_actor(logger)

data: TrackData = [
    {"col1": 2, "col2": 6.0, "col3": "FOO"},
    {"col1": 57, "col2": 7.0, "col3": "BAR"},
    {"col1": 2, "col2": 9.0, "col3": "FOO"},
    {"col1": 60, "col2": 1.1, "col3": "FOO"},
    {"col5": 6000},
]

request = LogRequest(
    dataset_id="model-48",
    timestamp=current_time_ms(),
    multiple=LogMultiple(
        columns=["col1", "col2", "col6"],
        data=[
            [2, 6.0, "FOO"],
            [57, 7.0, "BAR"],
            [2, 9.0, "FOO"],
            [60, 1.1, "FOO"],
        ],
    ),
)

print(
    bytes(request.json(), 'utf-8'),
)

# logger.log(data)
message = RawLogMessage(
    request=bytes(request.json(),  'utf-8'),
    request_time=current_time_ms(),
)
logger.send(message)
# logger.send(TrackMessage(data=data, timestamp_ms=current_time_ms(), result=None))
# logger.flush()
logger.shutdown()
