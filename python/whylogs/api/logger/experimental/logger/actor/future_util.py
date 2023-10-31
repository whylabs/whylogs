from concurrent.futures import Future, wait
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


def wait_result(future: "Future[T]", timeout: Optional[float] = None) -> T:
    """
    Wait on a future with an optional timeout.
    """
    done, not_done = wait([future], timeout=timeout)

    for timeouts in not_done:
        timeouts.set_exception(TimeoutError("Timeout waiting for result"))

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


def wait_result_while(future: "Future[T]", predicate: Callable[[], bool]) -> T:
    """
    Wait on a future while the condition is true.
    """
    result: Optional[T] = None
    while predicate():
        try:
            result = wait_result(future, 1.0)
        except TimeoutError:
            pass

    if result is None:
        raise TimeoutError("Wait signal stopped before result was available.")

    return result
