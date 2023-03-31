from typing import Callable, List

# A sequence of timers get instantiated, so we need global state for them
# to simulate universal time.

_FAKE_TIMES: List[float] = []  # list of times (seconds) where the FakeTimer checks if it should execute
_TIME_INDEX: int = 0  # where are we in the list of times


class FakeTimer:
    """
    Fake for threading.Timer, not threaded. Evaluates elapsed duration
    at times in the _FAKE_TIME list.
    """

    def __init__(self, interval: float, fn: Callable) -> None:
        self._interval = interval
        self._fn = fn
        self._prev_start_time = 0.0
        if _TIME_INDEX < len(_FAKE_TIMES):
            self._prev_start_time = _FAKE_TIMES[_TIME_INDEX]

    def start(self) -> None:
        global _TIME_INDEX
        _TIME_INDEX += 1
        if _TIME_INDEX >= len(_FAKE_TIMES):
            return

        now = _FAKE_TIMES[_TIME_INDEX]
        if (now - self._prev_start_time) >= self._interval:
            self._fn()
            self._prev_start_time = now
        else:
            self.start()

    def cancel(self) -> None:
        global _TIME_INDEX
        _TIME_INDEX = len(_FAKE_TIMES) + 1

    def is_alive(self) -> bool:
        return True
