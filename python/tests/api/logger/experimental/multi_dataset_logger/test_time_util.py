import time

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
    t = FunctionTimer(Schedule(TimeGranularity.Second, 1), f.inc)
    time.sleep(3)
    t.stop()

    assert f.i >= 3
