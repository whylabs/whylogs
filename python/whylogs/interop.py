import atexit
import copy
import logging
import os
from typing import Any, Dict, List

import coloredlogs
import pandas as pd
from faster_fifo import Queue
from py4j.clientserver import ClientServer, JavaParameters, PythonParameters
from py4j.java_collections import JavaList, ListConverter

from whylogs.api.logger.experimental.multi_dataset_logger.multi_dataset_rolling_logger import (
    MultiDatasetRollingLogger,
    TrackData,
)
from whylogs.api.logger.experimental.multi_dataset_logger.process_logger import (
    ProcessLogger,
)
from whylogs.api.logger.experimental.multi_dataset_logger.profile_actor_messages import (
    LogMessage,
    LogMultiple,
    LogRequest,
    LogRequestDict,
    RawLogMessage,
)
from whylogs.api.logger.experimental.multi_dataset_logger.time_util import (
    Schedule,
    TimeGranularity,
    current_time_ms,
)
from whylogs.api.writer.whylabs import WhyLabsWriter


class JavaProcess(object):
    def __init__(self) -> None:
        key = "hDQSx9nXzJ.viriMIcZYTnnlAob7wSl7Nbg18v84vISOS5uHYTzFVTNSvEplSlfO"
        writer = WhyLabsWriter(org_id="org-0", api_key=key, dataset_id="model-48")
        schedule = Schedule(cadence=TimeGranularity.Minute, interval=2)
        self.proc_logger = ProcessLogger(aggregate_by=TimeGranularity.Hour, write_schedule=schedule, writers=[writer])
        self.thread_logger = MultiDatasetRollingLogger(
            aggregate_by=TimeGranularity.Hour, write_schedule=schedule, writers=[writer]
        )
        self.proc_logger.start()

    def _log_process_logger(self, columns: JavaList, data: List[List[Any]]) -> None:
        pyColumns = [it for it in columns]
        pyData = [[it2 for it2 in it] for it in data]
        print(type(list(data)[0]))
        log: LogRequestDict = {
            "datasetId": "model-48",
            "timestamp": current_time_ms(),
            "multiple": {"columns": list(columns), "data": list(data)},
        }
        message = LogMessage(request_time=current_time_ms(), log=log)
        # self.proc_logger.send(message)

    def _log_thread_logger(self, columns: JavaList, data: List[List[Any]]) -> None:
        df = pd.DataFrame(data=data, columns=columns)
        self.thread_logger.log(df)

    def log(self, columns: JavaList, data: List[List[Any]]) -> None:
        # self._log_thread_logger(columns, data)
        self._log_process_logger(columns, data)

    class Java:
        implements = ["org.example.WhylogsLogger"]


def init_logging() -> None:
    coloredlogs.install()
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logging.basicConfig(handlers=[ch])
    logging.root.setLevel(logging.DEBUG)


init_logging()

proc = JavaProcess()


@atexit.register
def exit() -> None:
    proc.proc_logger.close()
    proc.thread_logger.close()


gateway = ClientServer(
    java_parameters=JavaParameters(),
    python_parameters=PythonParameters(),
    python_server_entry_point=proc,
)
