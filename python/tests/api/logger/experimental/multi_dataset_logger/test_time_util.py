from threading import Timer
from typing import Callable, List

from whylogs.api.logger.experimental.multi_dataset_logger.time_util import (
    FunctionTimer,
    Schedule,
    TimeGranularity,
)

# A sequence of timers get instantiated, so we need global state for them
# to simulate universal time.

_FAKE_TIMES: List[float] = []  # list of times (seconds) where the FakeTimer checks if it should execute
_TIME_INDEX: int = 0  # where are we in the list of times


class FakeTimer:
    def __init__(self, interval: int, fn: Callable) -> None:
        self._interval = interval
        self._fn = fn
        self._prev_start_time = 0
        if _TIME_INDEX < len(_FAKE_TIMES):
            self._prev_start_time = _FAKE_TIMES[_TIME_INDEX]
            self._zombie_thread = None
        else:
            self._zombie_thread = Timer(interval, fn)

    def start(self) -> None:
        global _TIME_INDEX
        _TIME_INDEX += 1
        if _TIME_INDEX >= len(_FAKE_TIMES):
            self._zombie_thread = Timer(self._interval, self._fn)
            self._zombie_thread.start()
            return

        now = _FAKE_TIMES[_TIME_INDEX]
        if (now - self._prev_start_time) >= self._interval:
            self._fn()
            self._prev_start_time = now
        else:
            self.start()

    def is_alive(self) -> bool:
        return self._zombie_thread.is_alive() if self._zombie_thread else True

    def cancel(self) -> None:
        if self._zombie_thread:
            self._zombie_thread.cancel()


def test_seconds_work() -> None:
    class Foo:
        def __init__(self) -> None:
            self.i = 0

        def inc(self) -> None:
            self.i += 1

    f = Foo()
    global _FAKE_TIMES, _TIME_INDEX
    _FAKE_TIMES = [0, 0.5, 1, 1.9, 2, 2.1, 3]
    _TIME_INDEX = 0
    t = FunctionTimer(Schedule(TimeGranularity.Second, 1), f.inc, FakeTimer)
    # time.sleep(3)
    t.stop()

    assert f.i >= 3
