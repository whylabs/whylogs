import asyncio
import atexit
import logging
import threading
from functools import partial, wraps
from typing import List

_NO_ASYNC = "WHYLOGS_NO_ASYNC"

_logger = logging.getLogger(__name__)

_threads: List[threading.Thread] = []


def _do_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


def async_wrap(func, *args, **kwargs):
    """

    Args:
        func: the coroutine to run in an asyncio loop

    Returns:
        threading.Thread: an thread for the coroutine
    """
    thread = threading.Thread(target=func, args=args, kwargs=kwargs)
    thread.start()

    _threads.append(thread)
    return thread


@atexit.register
def _wait_for_children():
    """
    Wait for the child process to complete. This is to ensure that we write out the log files before the parent
    process finishes
    """
    for t in _threads:
        try:
            t.join()
        except:  # noqa
            _logger.exception("Failed to await task")
