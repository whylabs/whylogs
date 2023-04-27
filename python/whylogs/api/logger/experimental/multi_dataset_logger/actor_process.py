from faster_fifo import Queue
import signal
from .signal_util import suspended_signals
from .list_util import type_batched_items
from typing import Optional
import time
import os
import logging
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Type, Union
from multiprocessing import Process, Event
from queue import Empty, Full

MessageType = TypeVar("MessageType")

_DEFAULT_POLL_WAIT_SECONDS = 0.1
_DEFAULT_QUEUE_SIZE_BYTES = 1000 * 1000 * 1000


class CloseMessage:
    pass


class ActorProcess(Process, ABC, Generic[MessageType]):
    def __init__(self, queue: Queue, auto_start: bool = True) -> None:
        # self.queue = Queue(_DEFAULT_QUEUE_SIZE_BYTES)
        self.queue = queue
        self._logger = logging.getLogger(f"{type(self).__name__}_{id(self)}")
        self._work_done_signal = Event()
        super().__init__()

        # if auto_start:
            # _start_logging_process(self)

    def send(self, message: Union[CloseMessage, MessageType]) -> None:
        if self.queue.is_closed():
            self._logger.warn("Dropping message because queue is closed.")
            return

        if isinstance(message, CloseMessage):
            self.queue.close()

        done = False
        while not done:
            try:
                self.queue.put(message, timeout=_DEFAULT_POLL_WAIT_SECONDS)
                done = True
            except Full:
                current_size = self.queue.qsize()
                if current_size == 0:
                    str_msg = str(message)
                    msg = str_msg[: min(len(str_msg), 400)]
                    self._logger.error(f"Message queue can't fit '{msg}...', dropping it permanently.")
                    done = True
                else:
                    self._logger.warning(f"Message queue full, trying again. Current size :{self.queue.qsize()}")
            except Exception as e:
                self._logger.exception(e)

    @abstractmethod
    def process_batch(self, batch: List[MessageType], batch_type: Type) -> None:
        pass

    @abstractmethod
    def after_start(self) -> None:
        pass

    def _polling_condition(self, batch_len: int, max: int, last_message_time: float, remaining: int) -> bool:
        if self._work_done_signal.is_set() and remaining == 0:
            self._logger.info("Stopping poll. Handled all messages and shutting down.")
            return False

        if batch_len >= max:
            self._logger.info(f"Stopping poll. Got {batch_len} messages.")
            return False

        if time.perf_counter() - last_message_time > 1:
            return False

        return True

    def _load_messages(self) -> Optional[List[MessageType]]:
        max = 50_000  # TODO make configurable
        batch: List[MessageType] = []
        last_message_time = time.perf_counter()

        while self._polling_condition(len(batch), max, last_message_time, self.queue.qsize()):
            try:
                batch += self.queue.get_many(timeout=0.1, max_messages_to_get=max)
            except Empty:
                if self.queue.is_closed():
                    self._logger.info("Queue closed and no more messages to process.")
                    return None if batch == [] else batch

        return batch

    def process_messages(self) -> None:
        messages: Optional[List[MessageType]] = []
        while messages is not None:
            messages = self._load_messages()

            if messages is None:
                continue

            for (batch, batch_type) in type_batched_items(messages):
                if batch == []:
                    continue

                self._logger.info(
                    f"Processing batch of {len(batch)} {batch_type.__name__}. {self.queue.qsize()} remaining"
                )

                try:
                    self.process_batch(batch, batch_type)
                except Exception as e:
                    self._logger.error("Error while processing messages")
                    self._logger.exception(e)

        # Can only get here if we're done processing messages
        self._work_done_signal.set()

    def run(self) -> None:
        self.after_start()
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
            self._logger.info("Process shutting down.")
            os._exit(0)  # Not sure why I need this but I definitely do

    def close(self) -> None:
        if self.pid is None:
            raise Exception("Process hasn't been started yet.")
        self._logger.info("Sending Close message to work queue.")
        self.send(CloseMessage())
        self._logger.info(
            f"Process will shutdown after all pending {self.queue.qsize()} data has been processed and uploaded."
        )
        self._work_done_signal.wait()


def _start_logging_process(proc: ActorProcess) -> None:
    proc.daemon = True
    proc.start()
    proc.join(0.1)
    print(f"Started process {proc.pid}")
