from typing import Generic, List, Optional, TypeVar

from whylogs.api.logger.experimental.logger.actor.actor import (
    _DEFAULT_TIMEOUT,
    QueueWrapper,
)

try:
    from faster_fifo import Queue as FasterQueue  # type: ignore
except ImportError:
    from whylogs.api.logger.experimental.logger.actor.proc_error_message import (
        _proc_error_message,
    )

    raise ImportError(_proc_error_message)

_DEFAULT_QUEUE_SiZE = 1000 * 1000 * 1000

FasterQueueMessageType = TypeVar("FasterQueueMessageType")


class FasterQueueWrapper(QueueWrapper, Generic[FasterQueueMessageType]):
    """
    Implementation of QueueWrapper sufficient for use in the threaded actor.
    """

    def __init__(self) -> None:
        self._queue = FasterQueue(_DEFAULT_QUEUE_SiZE)

    def send(self, message: FasterQueueMessageType, timeout: float = _DEFAULT_TIMEOUT) -> None:
        self._queue.put(message, timeout=timeout)

    def send_many(self, messages: List[FasterQueueMessageType], timeout: float = _DEFAULT_TIMEOUT) -> None:
        self._queue.put_many(messages, timeout=timeout)

    def get(self, timeout: float = _DEFAULT_TIMEOUT) -> Optional[FasterQueueMessageType]:
        return self._queue.get(timeout=timeout)

    def get_many(self, timeout: float = _DEFAULT_TIMEOUT, max: Optional[int] = None) -> List[FasterQueueMessageType]:
        return self._queue.get_many(timeout=timeout, max_messages_to_get=max)

    def size(self) -> int:
        return self._queue.qsize()

    def close(self) -> None:
        self._queue.close()
