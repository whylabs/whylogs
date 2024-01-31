import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from threading import Timer
from typing import Any, Callable, Type

from dateutil import tz


def current_time_ms() -> int:
    return time.time_ns() // 1_000_000


class TimeGranularity(Enum):
    Second = "Second"
    Minute = "Minute"
    Hour = "Hour"
    Day = "Day"
    Month = "Month"
    Year = "Year"


def truncate_time_ms(t: int, granularity: TimeGranularity) -> int:
    dt = datetime.fromtimestamp(t / 1000, tz=tz.tzutc()).replace(second=0, microsecond=0)

    if granularity == TimeGranularity.Minute:
        trunc = dt
    elif granularity == TimeGranularity.Hour:
        trunc = dt.replace(minute=0)
    elif granularity == TimeGranularity.Day:
        trunc = dt.replace(minute=0, hour=0)
    elif granularity == TimeGranularity.Month:
        trunc = dt.replace(minute=0, hour=0, day=1)
    elif granularity == TimeGranularity.Year:
        trunc = dt.replace(minute=0, hour=0, day=1, month=1)
    else:
        raise ValueError(f"Unsupported granularity: {granularity}")

    return int(trunc.timestamp() * 1000)


@dataclass
class Schedule:
    cadence: TimeGranularity
    interval: int


class FunctionTimer:
    """
    A timer that executes a function repeatedly given a Schedule. It will execute at the bottom of
    that time period. For example, a schedule of Schedule(TimeGranularity.Hour, 1) will execute at
    the start of each hour. If you start the timer 5 minutes before the next hour then it will first
    execute in five minutes, and then each hour after that.
    """

    def __init__(self, schedule: Schedule, fn: Callable[[], Any], timer_class: Type[Any] = Timer) -> None:
        self._logger = logging.getLogger(f"{type(self).__name__}_{id(self)}")
        self._fn = fn
        self._schedule = schedule
        self._running = True
        self._timer_class = timer_class
        now = datetime.now(tz=tz.tzutc())

        # Convert each of the intervals into seconds
        if schedule.cadence == TimeGranularity.Second:
            self.repeat_interval = schedule.interval
            next_second = (now + timedelta(seconds=self._schedule.interval)).replace(microsecond=0)
            initial_interval = min(0, (next_second - now).seconds)
        elif schedule.cadence == TimeGranularity.Minute:
            self.repeat_interval = schedule.interval * 60
            next_minute = (now + timedelta(minutes=self._schedule.interval)).replace(microsecond=0, second=0)
            initial_interval = (next_minute - now).seconds
        elif schedule.cadence == TimeGranularity.Hour:
            self.repeat_interval = schedule.interval * 60 * 60
            next_hour = (now + timedelta(hours=self._schedule.interval)).replace(microsecond=0, second=0, minute=0)
            initial_interval = (next_hour - now).seconds
        elif schedule.cadence == TimeGranularity.Day:
            self.repeat_interval = schedule.interval * 60 * 60 * 24
            next_day = (now + timedelta(days=self._schedule.interval)).replace(
                microsecond=0, second=0, minute=0, hour=0
            )
            initial_interval = (next_day - now).seconds
        elif schedule.cadence == TimeGranularity.Month:
            raise Exception("Can't use Monthly schedule.")
        elif schedule.cadence == TimeGranularity.Year:
            raise Exception("Can't use Yearly schedule.")
        else:
            raise Exception(f"Unsupported cadence {schedule.cadence}")

        self._logger.debug(f"scheduled for {initial_interval} seconds from now")
        self._timer = timer_class(initial_interval, self._run)
        self._timer.start()

    def _run(self) -> None:
        self._logger.debug(f"scheduled for {self.repeat_interval} seconds from now")
        self._timer = self._timer_class(self.repeat_interval, self._run)
        self._timer.start()
        self._fn()

    def is_alive(self) -> bool:
        return self._timer.is_alive()

    def stop(self) -> None:
        self._logger.info("Stopping timer")
        if not self._running:
            raise Exception("Timer already stopped")
        self._timer.cancel()
        self._running = False
