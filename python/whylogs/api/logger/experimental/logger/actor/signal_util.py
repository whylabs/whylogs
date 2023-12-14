import signal
from contextlib import contextmanager
from typing import Any


def _ignore(signal: int, frame: Any) -> None:
    pass


@contextmanager
def suspended_signals(*signals: signal.Signals) -> Any:
    """
    Suspends signal handling execution
    """
    for sig in signals:
        signal.signal(sig, _ignore)
    signal.pthread_sigmask(signal.SIG_BLOCK, set(signals))
    try:
        yield None
    finally:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, set(signals))
