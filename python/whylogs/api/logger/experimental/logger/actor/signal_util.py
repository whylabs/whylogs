import signal
from contextlib import contextmanager
from typing import Any


@contextmanager
def suspended_signals(*signals: Any) -> Any:
    """
    Suspends signal handling execution
    """
    signal.pthread_sigmask(signal.SIG_BLOCK, set(signals))
    try:
        yield None
    finally:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, set(signals))
