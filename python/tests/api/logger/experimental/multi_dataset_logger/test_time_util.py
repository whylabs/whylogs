from threading import Timer
from typing import Callable, List

import utils as u

from whylogs.api.logger.experimental.multi_dataset_logger.time_util import (
    FunctionTimer,
    Schedule,
    TimeGranularity,
    truncate_time_ms,
)


def test_truncate_day() -> None:
    ts = 1680038612000  # Tuesday, March 28, 2023 9:23:32 PM
    expected = 1679961600000  # Tuesday, March 28, 2023 0:00:00
    assert truncate_time_ms(ts, TimeGranularity.Day) == expected


def test_truncate_hour() -> None:
    ts = 1680038612000  # Tuesday, March 28, 2023 9:23:32 PM
    expected = 1680037200000  # Tuesday, March 28, 2023 21:00:00
    assert truncate_time_ms(ts, TimeGranularity.Hour) == expected


def test_truncate_month() -> None:
    ts = 1680038612000  # Tuesday, March 28, 2023 9:23:32 PM
    expected = 1677628800000  # Wednesday, March 1, 2023 0:00:00
    assert truncate_time_ms(ts, TimeGranularity.Month) == expected


def test_truncate_year() -> None:
    ts = 1680038612000  # Tuesday, March 28, 2023 9:23:32 PM
    expected = 1672531200000  # Sunday, January 1, 2023 0:00:00
    assert truncate_time_ms(ts, TimeGranularity.Year) == expected


def test_seconds_work() -> None:
    class Foo:
        def __init__(self) -> None:
            self.i = 0

        def inc(self) -> None:
            self.i += 1

    f = Foo()
    # Simulate running for 3 seconds, so it should fire 3 times
    u._FAKE_TIMES = [0, 0.5, 1, 1.9, 2, 2.1, 3]
    u._TIME_INDEX = 0
    t = FunctionTimer(Schedule(TimeGranularity.Second, 1), f.inc, u.FakeTimer)
    t.stop()

    assert f.i >= 3
