from concurrent.futures import Future, wait
from typing import TypeVar

T = TypeVar("T")


def wait_result(future: Future[T]) -> T:
    done, not_done = wait([future])
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
