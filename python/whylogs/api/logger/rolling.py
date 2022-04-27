import logging
import math
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from typing_extensions import Literal

from whylogs.api.logger.logger import Logger
from whylogs.core import DatasetProfile, DatasetSchema
from whylogs.core.stubs import pd

logger = logging.getLogger(__name__)


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
    ):
        super(TimedRollingLogger, self).__init__(schema)
        if base_name is None:
            base_name = "profile"
        if file_extension is None:
            file_extension = ".bin"

        self._current_profile: DatasetProfile = None  # type: ignore

        self.file_extension = file_extension
        self.base_name = base_name
        self.aligned = aligned
        self.fork = fork

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
        self.rollover_at = self.compute_next_rollover()

    def compute_next_rollover(self) -> int:
        t = int(time.time())
        if self.aligned:
            t = int(math.floor((t - 1) / self.interval)) * self.interval + self.interval
        return t + self.interval

    def _get_matching_profiles(
        self, obj: Any = None, *, pandas: Optional[pd.DataFrame] = None, row: Optional[Dict[str, Any]] = None
    ) -> List[DatasetProfile]:
        if self._shouldRollover():
            self._do_rollover()

        return [self._current_profile]

    def _shouldRollover(self) -> bool:
        """Determine if rollover should occur."""
        if self._current_profile is None:
            return True
        t = int(time.time())
        if t >= self.rollover_at:
            return True
        return False

    def _do_rollover(self) -> None:
        old_profile = self._current_profile
        dataset_timestamp = datetime.utcfromtimestamp(self.rollover_at - self.interval)
        self._current_profile = DatasetProfile(
            schema=self._schema,
            dataset_timestamp=dataset_timestamp,
        )
        self.rollover_at = self.compute_next_rollover()
        self._flush(old_profile)

    def _flush(self, profile: DatasetProfile) -> None:
        if profile is None:
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
            t = self.rollover_at - self.interval
            if self.utc:
                time_tuple = time.gmtime(t)
            else:
                time_tuple = time.localtime(t)
                current_time = int(time.time())

                dst_now = time.localtime(current_time)[-1]
                dst_then = time_tuple[-1]
                if dst_now != dst_then:
                    if dst_now:
                        addend = 3600
                    else:
                        addend = -3600
                    time_tuple = time.localtime(t + addend)
            timed_filename = f"{self.base_name}.{time.strftime(self.suffix, time_tuple)}{self.file_extension}"
            logging.debug("Writing out put with timed_filename: %s", timed_filename)

            for w in self._writers:
                w.write(profile=profile.view(), dest=timed_filename)

    def close(self) -> None:
        logging.debug("Closing the writer")
        if not self._is_closed:
            self._do_rollover()
        self._is_closed = True
