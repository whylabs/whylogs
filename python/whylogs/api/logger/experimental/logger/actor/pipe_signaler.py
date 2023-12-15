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
    ... [Your existing docstring] ...
    """

    def __init__(self) -> None:
        super().__init__()
        self.daemon = True
        self._logger = logging.getLogger(__name__)
        self._queue_config = QueueConfig()
        self.queue: FasterQueueWrapper[Tuple[str, Optional[Exception], Optional[T]]] = FasterQueueWrapper(self._queue_config)
        self.futures: Dict[str, "Future[Any]"] = {}
        self._end_polling = th.Event()
        self._done = th.Event()

    def signal(self, result: Tuple[str, Optional[Exception], Optional[T]]) -> None:
        """
        ... [Your existing docstring] ...
        """
        self.queue.send(result)

    def signal_many(self, results: List[Tuple[str, Optional[Exception], Optional[T]]]) -> None:
        self.queue.send_many(results)

    def register(self, future: "Future[T]", message_id: str) -> None:
        """
        ... [Your existing docstring] ...
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

    def close_child(self) -> None:
        """
        This method is no longer needed as queues do not require manual closing of file descriptors.
        """

    def close(self) -> None:
        """
        Closes the thread and all resources. This should be
        called from the parent side.
        """
        self._end_polling.set()
        self._done.wait()
        self.queue.close()  # type: ignore[reportUnknownMemberType]
        self.join()
