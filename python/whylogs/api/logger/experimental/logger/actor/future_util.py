from concurrent.futures import Future, wait
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


def _wait_result(future: "Future[T]", timeout: Optional[float] = None) -> T:
    """
    Wait on a future with an optional timeout without side effects. This won't update
    the status of the future for errors/timeouts.
    """
    done, not_done = wait([future], timeout=timeout)

    if len(not_done) > 0:
        raise TimeoutError("Timeout waiting for result")

    all = done.union(not_done)
    for it in all:
        e = it.exception()
        r = it.result()

        if e is not None:
            raise e
        elif it.cancelled():
            raise Exception("cancelled")
        else:
            return r

    raise Exception("Couldn't find a result")


def wait_result(future: "Future[T]", timeout: Optional[float] = None) -> T:
    """
    Wait on a future with an optional timeout.
    """
    try:
        return _wait_result(future, timeout=timeout)
    except TimeoutError as e:
        future.set_exception(e)
        raise e


def wait_result_while(future: "Future[T]", predicate: Callable[[], bool]) -> T:
    """
    Wait on a future while the condition is true.
    """
    try:
        while predicate():
            try:
                return _wait_result(future, 1.0)
            except TimeoutError:
                pass
    except Exception as e:
        if future.exception() is None:
            future.set_exception(e)
        raise e

    raise TimeoutError("Wait signal stopped before result was available.")
