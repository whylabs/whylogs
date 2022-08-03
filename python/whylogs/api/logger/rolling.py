import atexit
import logging
import math
import os
import time
from datetime import datetime
from threading import Timer
from typing import Any, Callable, Dict, List, Optional

from typing_extensions import Literal

from whylogs.api.logger.logger import Logger
from whylogs.api.writer import Writer
from whylogs.core import DatasetProfile, DatasetProfileView, DatasetSchema
from whylogs.core.stubs import pd

logger = logging.getLogger(__name__)


class Scheduler(object):
    """
    Multithreading scheduler.

    Schedule a function to be called repeatedly based on a schedule.
    """

    _timer: Timer

    def __init__(self, initial: float, interval: float, function: Callable, *args: Any, **kwargs: Any):
        self.initial = initial
        self._ran_initial = False
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self) -> None:
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self) -> None:
        if not self.is_running:
            interval = self.interval
            if not self._ran_initial:
                interval = self.initial
                self._ran_initial = True
            self._timer = Timer(interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self) -> None:
        self._timer.cancel()
        self.is_running = False


class TimedRollingLogger(Logger):
    """A rolling logger that continuously rotates files based on time."""

    def __init__(
        self,
        schema: Optional[DatasetSchema] = None,
        *,
        base_name: Optional[str] = None,
        file_extension: Optional[str] = None,
        interval: int = 1,
        when: Literal["S", "M", "H", "D"] = "H",
        utc: bool = False,
        aligned: bool = True,
        fork: bool = False,
        skip_empty: bool = False,
        callback: Optional[Callable[[Writer, DatasetProfileView, str], None]] = None,
    ):
        super().__init__(schema)
        if base_name is None:
            base_name = "profile"
        if file_extension is None:
            file_extension = ".bin"

        self.file_extension = file_extension
        self.base_name = base_name
        self.aligned = aligned
        self.fork = fork
        self.skip_empty = skip_empty

        # base on TimedRotatingFileHandler
        self.when = when.upper()
        if self.when == "S":
            self.interval = 1  # one second
            self.suffix = "%Y-%m-%d_%H-%M-%S"
        elif self.when == "M":
            self.interval = 60  # one minute
            self.suffix = "%Y-%m-%d_%H-%M"
        elif self.when == "H":
            self.interval = 60 * 60  # one hour
            self.suffix = "%Y-%m-%d_%H"
        elif self.when == "D" or self.when == "MIDNIGHT":
            self.interval = 60 * 60 * 24  # one day
            self.suffix = "%Y-%m-%d"
        self.interval = self.interval * interval  # multiply by units requested
        self.utc = utc

        now = time.time()
        self._current_batch_timestamp = self._compute_current_batch_timestamp(now)

        self._current_profile: DatasetProfile = DatasetProfile(
            schema=schema,
            dataset_timestamp=datetime.utcfromtimestamp(self._current_batch_timestamp),
        )

        initial_run_after = (self._current_batch_timestamp + self.interval) - now
        if initial_run_after <= 0:
            logger.error(
                "Negative initial run after. This shouldn't happen so something went wrong with the clock here"
            )
            initial_run_after = self.interval
        self._callback = callback
        self._scheduler = Scheduler(initial_run_after, interval=self.interval, function=self._do_rollover)

        self._scheduler.start()

        atexit.register(self.close)

    def check_writer(self, writer: Writer) -> None:
        writer.check_interval(self.interval)

    def _compute_current_batch_timestamp(self, now: Optional[float] = None) -> int:
        if now is None:
            now = time.time()
        rounded_now = int(now)
        if self.aligned:
            return int(math.floor((rounded_now - 1) / self.interval)) * self.interval + self.interval
        return rounded_now

    def _get_matching_profiles(
        self, obj: Any = None, *, pandas: Optional[pd.DataFrame] = None, row: Optional[Dict[str, Any]] = None
    ) -> List[DatasetProfile]:
        return [self._current_profile]

    def _do_rollover(self) -> None:
        if self._is_closed:
            return
        old_profile = self._current_profile

        self._current_batch_timestamp = self._compute_current_batch_timestamp()
        dataset_timestamp = datetime.utcfromtimestamp(self._current_batch_timestamp)
        self._current_profile = DatasetProfile(
            schema=self._schema,
            dataset_timestamp=dataset_timestamp,
        )

        self._flush(old_profile)

    def _flush(self, profile: DatasetProfile) -> None:
        if profile is None:
            return
        if self.skip_empty and profile.is_empty:
            logger.debug("skip_empty is set. Skipping empty profiles")
            return

        pid = 0
        if self.fork:
            pid = os.fork()

        if pid > 0:
            logger.debug("Forked child process. Child process ID: %d", pid)
        else:
            if self.fork:
                logger.debug("In child process")
            else:
                logger.debug("Didn't fork. Writing in the same process")
            if self.utc:
                time_tuple = time.gmtime(self._current_batch_timestamp)
            else:
                time_tuple = time.localtime(self._current_batch_timestamp)
                current_time = int(time.time())

                dst_now = time.localtime(current_time)[-1]
                dst_then = time_tuple[-1]
                if dst_now != dst_then:
                    if dst_now:
                        addend = 3600
                    else:
                        addend = -3600
                    time_tuple = time.localtime(self._current_batch_timestamp + addend)
            timed_filename = f"{self.base_name}.{time.strftime(self.suffix, time_tuple)}{self.file_extension}"
            logging.debug("Writing out put with timed_filename: %s", timed_filename)

            while profile.is_active:
                time.sleep(1)

            for w in self._writers:
                w.write(profile=profile.view(), dest=timed_filename)
                if self._callback and callable(self._callback):
                    self._callback(w, profile.view(), timed_filename)

    def close(self) -> None:
        logging.debug("Closing the writer")
        if not self._is_closed:
            self._scheduler.stop()
            self._do_rollover()
        self._is_closed: bool = True
