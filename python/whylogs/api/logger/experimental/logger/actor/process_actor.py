import multiprocessing as mp
import signal
import sys
from enum import Enum
from typing import Generic, TypeVar

from whylogs.api.logger.experimental.logger.actor.actor import (
    Actor,
    QueueConfig,
    QueueWrapper,
)
from whylogs.api.logger.experimental.logger.actor.signal_util import suspended_signals


class QueueType(Enum):
    MP = "MP"
    FASTER_FIFO = "FASTER_FIFO"


ProcessMessageType = TypeVar("ProcessMessageType")


class ProcessActor(Actor, mp.Process, Generic[ProcessMessageType]):
    """
    Subclass of Actor that uses a process to process messages.
    """

    def __init__(
        self, queue_config: QueueConfig = QueueConfig(), queue_type: QueueType = QueueType.FASTER_FIFO
    ) -> None:
        self._wrapper: QueueWrapper[ProcessMessageType]
        if queue_type == QueueType.MP:
            from whylogs.api.logger.experimental.logger.actor.mp_queue_wrapper import (
                MPQueueWrapper,
            )

            self._wrapper = MPQueueWrapper()
        elif queue_type == QueueType.FASTER_FIFO:
            from whylogs.api.logger.experimental.logger.actor.faster_fifo_queue_wrapper import (
                FasterQueueWrapper,
            )

            self._wrapper = FasterQueueWrapper(queue_config)
        else:
            raise ValueError(f"Unknown queue type: {queue_type}")

        self._event = mp.Event()
        self._is_closed = mp.Event()
        self._close_handled = mp.Event()
        # our mypy version has a false positive on this super call
        super().__init__(self._wrapper, queue_config)  # type: ignore

    def close_message_handled(self) -> bool:
        return self._close_handled.is_set()

    def set_close_message_handled(self) -> None:
        self._close_handled.set()

    def close_message_wait(self) -> None:
        self._close_handled.wait()

    def is_done(self) -> bool:
        return self._event.is_set()

    def done_wait(self) -> None:
        self._event.wait()

    def set_done(self) -> None:
        self._event.set()

    def set_closed(self) -> None:
        self._is_closed.set()

    def is_closed(self) -> bool:
        # Include is_alive here to try to make the exit more graceful when something crazy happens that results
        # in the process being killed.
        return not self.is_alive() or self._is_closed.is_set()

    def close(self) -> None:
        if self.pid is None:
            raise Exception("Process hasn't been started yet.")

        if not self.is_alive():
            raise Exception("Process isn't active. It might have been killed.")

        super().close()
        self._wrapper.close()

    def run(self) -> None:
        try:
            with suspended_signals(signal.SIGINT, signal.SIGTERM):
                self.process_messages()
        except KeyboardInterrupt:
            # Swallow this to prevent annoying stack traces in dev.
            self._logger.info("Keyboard interrupt ignored in sub process.")
        except Exception as e:
            self._logger.error("Error while in main processing loop")
            self._logger.exception(e)

        sys.exit(0)

    def start(self) -> None:
        """
        The process version of the actor apparently has to be manually started after
        it's created, unlike the thread version which can just be automatically started
        from within its init. There must be some post-init setup that needs to be done.
        """
        self.daemon = True
        super().start()
        self.join(0.1)  # This does apparently need to happen after several manual tests.
