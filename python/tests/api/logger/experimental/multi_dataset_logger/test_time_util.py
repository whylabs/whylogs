import time

from whylogs.api.logger.experimental.multi_dataset_logger.time_util import (
    FunctionTimer,
    Schedule,
    TimeGranularity,
)


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
