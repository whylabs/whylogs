import logging
import threading as th
from concurrent.futures import Future
from queue import Empty
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

from whylogs.api.logger.experimental.logger.actor.actor import QueueConfig
from whylogs.api.logger.experimental.logger.actor.faster_fifo_queue_wrapper import (
    FasterQueueWrapper,
)

T = TypeVar("T")


class PipeSignaler(th.Thread, Generic[T]):
    """
    A thread that listens on a pipe for messages and signals the corresponding futures.
    This class is used in the process logger to enable synchronous logging requests across processes.
    It's essentially a dictionary of futures that are registered by the main process and signaled by the
    child process. A lot of the behavior is implicit because it involves properties of processes, so it's
    worth documenting here.
    - This thread has to be started from the main process, which means it has to be started right before the
        process logger is started (before the os.fork under the hood). It has to be started from the main process
        because the main process will be registering futures on it, and those can't cross the process boundary.
    - The parent and child process each have references to the pipes and they each need to close their references,
        which means close_child has to be called from the child process and close has to be called from the parent.
        Calling close_child in the main processing code will have right effect.
    - The process actor does message batching so multiple ids may be signaled even though a single batch was processed
        because that batch could have contained multiple messages.
    - The signaler uses Events under the hood to know when to stop working. They can be th.Events even though this
        is being used in a multiprocessing environment because nothing the child does can affect them. Keep in mind
        that introducing any behavior on the child side that depends on knowing whether those events are set won't work
        though, they would have to be switched to mp.Events for that.
    This class should really never be used by anyone in most cases. It will just slow down the main process by making
    it wait for logging to complete, but it enables a lot of testing and debugging.
    """

    def __init__(self) -> None:
        super().__init__()
        self.daemon = True
        self._logger = logging.getLogger(__name__)
        self._queue_config = QueueConfig()
        self.queue: FasterQueueWrapper[Tuple[str, Optional[Exception], Optional[T]]] = FasterQueueWrapper(
            self._queue_config
        )
        self.futures: Dict[str, "Future[Any]"] = {}
        self._end_polling = th.Event()
        self._done = th.Event()

    def signal(self, result: Tuple[str, Optional[Exception], Optional[T]]) -> None:
        """
        Signal that a message was handled by sending a tuple of (message id, exception, data).
        data and exception can be None.
        This should be called from the child process.
        """
        self.queue.send(result)

    def signal_many(self, results: List[Tuple[str, Optional[Exception], Optional[T]]]) -> None:
        self.queue.send_many(results)

    def register(self, future: "Future[T]", message_id: str) -> None:
        """
        Register a future to be signaled when the message id is received.
        This should be called from the parent process.
        """
        self._logger.debug(f"Received register request for id {message_id}")
        self.futures[message_id] = future

    def _start_poll_conn(self) -> None:
        while not self._end_polling.is_set():
            try:
                messages = self.queue.get_many(
                    timeout=self._queue_config.message_poll_wait,
                    max=self._queue_config.max_batch_size,
                )

                for message_id, exception, data in messages:
                    self._logger.debug(f"Received message id {message_id}")
                    future: Optional["Future[Any]"] = self.futures.pop(message_id, None)
                    if future is not None:
                        self._logger.debug(f"Setting result for message id {message_id} {exception}")
                        if exception is not None:
                            future.set_exception(exception)
                        else:
                            # Doing this in the else avoid testing `data is None`, which would be
                            # wrong because data can potentially be None if T is Optional
                            future.set_result(data)

            except Empty:
                continue
            except Exception as e:
                self._logger.exception(f"Error in queue {e}")
                break

        self._done.set()

    def run(self) -> None:
        self._start_poll_conn()

    def close(self) -> None:
        """
        Closes the thread and all resources. This should be
        called from the parent side.
        """
        self._end_polling.set()
        self._done.wait()
        self.queue.close()  # type: ignore[reportUnknownMemberType]
        self.join()
