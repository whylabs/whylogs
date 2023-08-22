import logging
import queue
import signal
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, List, Optional, Type, TypeVar, Union

from whylogs.api.logger.experimental.logger.actor.list_util import type_batched_items
from whylogs.api.logger.experimental.logger.actor.signal_util import suspended_signals

QueueMessageType = TypeVar("QueueMessageType")
_DEFAULT_TIMEOUT = 0.1


class QueueWrapper(Generic[QueueMessageType]):
    @abstractmethod
    def send(self, message: QueueMessageType, timeout: float = _DEFAULT_TIMEOUT) -> None:
        raise NotImplementedError()

    @abstractmethod
    def send_many(self, message: QueueMessageType, timeout: float = _DEFAULT_TIMEOUT) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get(self, timeout: float = _DEFAULT_TIMEOUT) -> Optional[QueueMessageType]:
        raise NotImplementedError()

    @abstractmethod
    def get_many(self, timeout: float = _DEFAULT_TIMEOUT, max: Optional[int] = None) -> List[QueueMessageType]:
        raise NotImplementedError()

    @abstractmethod
    def size(self) -> int:
        raise NotImplementedError()

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError()


class CloseMessage:
    pass


@dataclass
class QueueConfig:
    """
    Configuration for the queue used by the actor.

    Parameters
    ----------
    max_batch_size : int
        Maximum number of messages to process in a single batch.
    message_accumualtion_duration : float
        How long to accumulate messages before processing a batch
    message_poll_wait : float
        How long to block when polling for messages in a loop.
    """

    max_batch_size: int = 50_000
    message_accumualtion_duration: float = 1.0  # seconds
    message_poll_wait: float = 0.1  # seconds


MessageType = TypeVar("MessageType")
Messages = Union[MessageType, CloseMessage]


class Actor(ABC, Generic[MessageType]):
    """
    Simple actor inspired interface designed for batch processing messages.
    This was created with thread and process based whylogs logging in mind and it has
    all of the common message passing logic needed.
    """

    def __init__(
        self,
        queue_wrapper: QueueWrapper[Messages],
        queue_config: QueueConfig = QueueConfig(),
    ) -> None:
        self._queue: QueueWrapper[Messages] = queue_wrapper
        self._logger = logging.getLogger(f"ai.whylabs.actor.{type(self).__name__}")
        self._logger.setLevel(logging.DEBUG)
        self._queue_config = queue_config
        super().__init__()

    @abstractmethod
    def close_message_handled(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def set_close_message_handled(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def close_message_wait(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def is_done(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def done_wait(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def set_done(self) -> None:
        """
        Set this actor as done, meaning it has finished processing all messages.
        """
        raise NotImplementedError()

    @abstractmethod
    def is_closed(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def set_closed(self) -> None:
        """
        Sets this actor as closed, meaning it should no longer accept messages.
        """
        raise NotImplementedError()

    @abstractmethod
    def process_batch(self, batch: List[Messages], batch_type: Type) -> None:
        raise NotImplementedError()

    def send(self, message: Messages) -> None:
        if self.is_closed():
            raise Exception("Actor is closed, can't send message.")

        if isinstance(message, CloseMessage):
            self.set_closed()

        done = False
        while not done:
            try:
                self._queue.send(message, timeout=self._queue_config.message_poll_wait)
                done = True
            except queue.Full:
                current_size = self._queue.size()
                if current_size == 0:
                    str_msg = str(message)
                    msg = str_msg[: min(len(str_msg), 400)]
                    self._logger.error(
                        f"Message queue can't fit '{msg}...', dropping it permanently. Message was too big to ever fit."
                    )
                    done = True
                else:
                    self._logger.warning(f"Message queue full, trying again. Current size :{self._queue.size()}")
            except Exception as e:
                self._logger.exception(e)

    def send_many(self, messages: List[Messages]) -> None:
        self._queue.send_many(messages)

    def close(self) -> None:
        self._logger.info("Sending Close message.")
        self.send(CloseMessage())
        self._logger.info(f"Shutdown after all pending {self._queue.size()} messages have been processed.")
        self.done_wait()

    def _polling_condition(self, batch_len: int, max: int, last_message_time: float, remaining: int) -> bool:
        if self.is_done() and remaining == 0:
            self._logger.info("Stopping poll. Handled all messages and shutting down.")
            return False

        if batch_len >= max:
            self._logger.info(f"Stopping poll. Got {batch_len} messages.")
            return False

        if time.perf_counter() - last_message_time > self._queue_config.message_accumualtion_duration:
            return False

        return True

    def _load_messages(self) -> Optional[List[Messages]]:
        max = self._queue_config.max_batch_size
        batch: List[Messages] = []
        last_message_time = time.perf_counter()

        while self._polling_condition(len(batch), max, last_message_time, self._queue.size()):
            try:
                batch += self._queue.get_many(timeout=self._queue_config.message_poll_wait, max=max)
                self._logger.info(f"Got {len(batch)} messages. {self._queue.size()} remaining")
            except queue.Empty:
                if self.is_closed() and self.close_message_handled():
                    self._logger.info("Queue closed and no more messages to process.")
                    return None if batch == [] else batch

        return batch

    def process_messages(self) -> None:
        messages: Optional[List[Messages]] = []
        while messages is not None:
            messages = self._load_messages()

            if messages is None:
                continue

            for batch, batch_type in type_batched_items(messages):
                if batch == []:
                    continue

                self._logger.info(
                    f"Processing batch of {len(batch)} {batch_type.__name__}. {self._queue.size()} remaining"
                )

                try:
                    self.process_batch(batch, batch_type)

                    if batch_type == CloseMessage:
                        self.set_close_message_handled()

                except Exception as e:
                    self._logger.error("Error while processing messages")
                    self._logger.exception(e)

        # Can only get here if we're done processing messages
        self.set_done()

    def run(self) -> None:
        try:
            with suspended_signals(signal.SIGINT, signal.SIGTERM):
                self.process_messages()
        except KeyboardInterrupt:
            # Swallow this to prevent annoying stack traces in dev.
            pass
        except Exception as e:
            self._logger.error("Error while in main processing loop")
            self._logger.exception(e)
        finally:
            self._logger.info("Shutting down.")
