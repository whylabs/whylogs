import queue
import threading as th
from typing import Generic, List, Optional, TypeVar

from whylogs.api.logger.experimental.logger.actor.actor import (
    DEFAULT_TIMEOUT,
    Actor,
    QueueConfig,
    QueueWrapper,
)

DefaultQueueWrapperType = TypeVar("DefaultQueueWrapperType")


class ThreadQueueWrapper(QueueWrapper[DefaultQueueWrapperType], Generic[DefaultQueueWrapperType]):
    """
    Implementation of QueueWrapper sufficient for use in the threaded actor.
    """

    def __init__(self) -> None:
        self._queue: queue.Queue[DefaultQueueWrapperType] = queue.Queue(100_000)

    def send(self, message: DefaultQueueWrapperType, timeout: float = DEFAULT_TIMEOUT) -> None:
        self._queue.put(message, timeout=timeout)

    def send_many(self, messages: List[DefaultQueueWrapperType], timeout: float = DEFAULT_TIMEOUT) -> None:
        for message in messages:
            self._queue.put(message, timeout=timeout)

    def get(self, timeout: float = DEFAULT_TIMEOUT) -> Optional[DefaultQueueWrapperType]:
        return self._queue.get(timeout=timeout)

    def get_many(self, timeout: float = DEFAULT_TIMEOUT, max: Optional[int] = None) -> List[DefaultQueueWrapperType]:
        if max is None or max < 1:
            return []

        messages: List[DefaultQueueWrapperType] = []

        while True:
            if len(messages) >= max:
                break

            try:
                messages.append(self._queue.get(timeout=timeout))
            except queue.Empty:
                if len(messages) == 0:
                    raise queue.Empty
                else:
                    break

        return messages

    def size(self) -> int:
        return self._queue.qsize()

    def close(self) -> None:
        pass


ThreadMessageType = TypeVar("ThreadMessageType")


class ThreadActor(Actor[ThreadMessageType], th.Thread, Generic[ThreadMessageType]):
    """
    Subclass of Actor that uses a thread to process messages.
    """

    def __init__(self, queue_config: QueueConfig = QueueConfig()) -> None:
        # mypy can't infer but pyright can
        super().__init__(ThreadQueueWrapper(), queue_config)  # type: ignore
        self._event = th.Event()
        self._is_closed = th.Event()
        self._close_handled = th.Event()
        self.daemon = True
        self.start()

    def run(self) -> None:
        super().run()

    def close_message_handled(self) -> bool:
        return self._close_handled.is_set()

    def set_close_message_handled(self) -> None:
        self._close_handled.set()

    def close_message_wait(self) -> None:
        self._close_handled.wait()

    def is_done(self) -> bool:
        return self._event.is_set()

    def set_done(self) -> None:
        self._event.set()

    def done_wait(self) -> None:
        self._event.wait()

    def set_closed(self) -> None:
        self._is_closed.set()

    def is_closed(self) -> bool:
        return self._is_closed.is_set()

    def close(self) -> None:
        super().close()
