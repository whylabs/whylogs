from whylogs.api.logger.experimental.multi_dataset_logger.actor_process import _start_logging_process
from faster_fifo import Queue
from whylogs.api.logger.experimental.multi_dataset_logger.multi_dataset_rolling_logger import MultiDatasetRollingLogger, TrackData, TrackMessage
from whylogs.api.logger.experimental.multi_dataset_logger.process_logger import ProfileActor
from whylogs.api.logger.experimental.multi_dataset_logger.time_util import TimeGranularity, Schedule, current_time_ms
from whylogs.api.writer.whylabs import WhyLabsWriter

import os, getpass
import logging

def init_logging():
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # formatter = DefaultFormatter("%(levelprefix)s [%(asctime)s %(name)s] %(message)s", datefmt="%d-%m-%Y-%H:%M:%S")
    # ch.setFormatter(formatter)

    logging.basicConfig(handlers=[ch])
    logging.root.setLevel(logging.DEBUG)

init_logging()

print(f'PID: {os.getpid()}')

os.environ["WHYLABS_DEFAULT_ORG_ID"] = "org-0"
os.environ["WHYLABS_API_KEY"] = "hDQSx9nXzJ.viriMIcZYTnnlAob7wSl7Nbg18v84vISOS5uHYTzFVTNSvEplSlfO"

# Create a logger that will write hourly profiles to WhyLabs every 10 minutes
writer = WhyLabsWriter(
    org_id=os.environ["WHYLABS_DEFAULT_ORG_ID"], 
    api_key=os.environ["WHYLABS_API_KEY"], 
    dataset_id="model-48"
)
schedule = Schedule(cadence=TimeGranularity.Minute, interval=1)

# logger = MultiDatasetRollingLogger(
#     aggregate_by=TimeGranularity.Hour, 
#     write_schedule=schedule,
#     writers=[writer],
#     # Can optionally provide a dataset schema here as well.
# )


queue = Queue(1000*1000)
logger = ProfileActor(
    aggregate_by=TimeGranularity.Hour, 
    write_schedule=schedule,
    writers=[writer],
    queue=queue
    # Can optionally provide a dataset schema here as well.
)
_start_logging_process(logger)

data: TrackData = [
    {'col1': 2, 'col2': 6.0, 'col3': 'FOO'},
    {'col1': 57, 'col2': 7.0, 'col3': 'BAR'},
    {'col1': 2, 'col2': 9.0, 'col3': 'FOO'},
    {'col1': 60, 'col2': 1.1, 'col3': 'FOO'},
    {'col5': 6000},
]

logger.log(data)
# logger.send(TrackMessage(data=data, timestamp_ms=current_time_ms(), result=None))
# logger.flush()
logger.close()