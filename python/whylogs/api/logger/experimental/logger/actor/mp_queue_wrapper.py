from multiprocessing import Queue
from queue import Empty
from typing import Generic, List, Optional, TypeVar

from whylogs.api.logger.experimental.logger.actor.actor import (
    DEFAULT_TIMEOUT,
    QueueWrapper,
)

MPWrapperType = TypeVar("MPWrapperType")


class MPQueueWrapper(QueueWrapper[MPWrapperType], Generic[MPWrapperType]):
    """
    Implementation of QueueWrapper sufficient for use in the threaded actor.
    """

    def __init__(self) -> None:
        self._queue: Queue[MPWrapperType] = Queue(100_000)

    def send(self, message: MPWrapperType, timeout: float = DEFAULT_TIMEOUT) -> None:
        self._queue.put(message, timeout=timeout)

    def send_many(self, messages: List[MPWrapperType], timeout: float = DEFAULT_TIMEOUT) -> None:
        for message in messages:
            self._queue.put(message, timeout=timeout)

    def get(self, timeout: float = DEFAULT_TIMEOUT) -> Optional[MPWrapperType]:
        return self._queue.get(timeout=timeout)

    def get_many(self, timeout: float = DEFAULT_TIMEOUT, max: Optional[int] = None) -> List[MPWrapperType]:
        if max is None or max < 1:
            return []

        messages: List[MPWrapperType] = []

        while True:
            if len(messages) >= max:
                break

            try:
                messages.append(self._queue.get(timeout=timeout))
            except Empty:
                if len(messages) == 0:
                    raise Empty
                else:
                    break

        return messages

    def size(self) -> int:
        return self._queue.qsize()

    def close(self) -> None:
        self._queue.close()
