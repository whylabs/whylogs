from typing import Any, Generic, List, Optional, TypeVar

from whylogs.api.logger.experimental.logger.actor.actor import (
    DEFAULT_TIMEOUT,
    QueueConfig,
    QueueWrapper,
)

try:
    from faster_fifo import Queue as FasterQueue  # type: ignore
except ImportError:
    from whylogs.api.logger.experimental.logger.actor.proc_error_message import (
        proc_error_message,
    )

    raise ImportError(proc_error_message)


FasterQueueMessageType = TypeVar("FasterQueueMessageType")


class FasterQueueWrapper(QueueWrapper[FasterQueueMessageType], Generic[FasterQueueMessageType]):
    """
    Implementation of QueueWrapper sufficient for use in the threaded actor.
    """

    def __init__(self, config: QueueConfig) -> None:
        # fasterfifo doesn't have types
        self._queue: Any = FasterQueue(config.max_buffer_bytes)

    def send(self, message: FasterQueueMessageType, timeout: float = DEFAULT_TIMEOUT) -> None:
        self._queue.put(message, timeout=timeout)

    def send_many(self, messages: List[FasterQueueMessageType], timeout: float = DEFAULT_TIMEOUT) -> None:
        self._queue.put_many(messages, timeout=timeout)

    def get(self, timeout: float = DEFAULT_TIMEOUT) -> Optional[FasterQueueMessageType]:
        return self._queue.get(timeout=timeout)

    def get_many(self, timeout: float = DEFAULT_TIMEOUT, max: Optional[int] = None) -> List[FasterQueueMessageType]:
        return self._queue.get_many(timeout=timeout, max_messages_to_get=max)

    def size(self) -> int:
        return self._queue.qsize()

    def close(self) -> None:
        self._queue.close()
