import logging
from abc import ABC, abstractmethod
from queue import Empty, Full, Queue
from threading import Event, Thread
from typing import Generic, TypeVar, Union


class CloseMessage:
    pass


MessageType = TypeVar("MessageType")


class MessageProcessor(ABC, Generic[MessageType], Thread):
    def __init__(self) -> None:
        super().__init__()
        self._logger = logging.getLogger(f"{type(self).__name__}_{id(self)}")
        self._queue: Queue[Union[MessageType, CloseMessage]] = Queue(100_000)
        self.daemon = True
        self._is_done = Event()
        self._closed = False
        self.start()

    def run(self) -> None:
        self._process_messages()

    def _process_messages(self) -> None:
        while not self._is_done.is_set():
            try:
                message = self._queue.get(timeout=0.1)
                self._logger.debug(f"Handling message {type(message).__name__}")
                self._process_message(message)
            except Empty:
                if self._closed:
                    self._logger.info("Done processing message queue")
                    self._is_done.set()
            except Full:
                self._logger.warning("Message queue full")
            except Exception as e:
                self._logger.exception(e)

    @abstractmethod
    def _process_message(self, message: Union[MessageType, CloseMessage]) -> None:
        pass

    def send(self, message: Union[MessageType, CloseMessage]) -> None:
        if self._closed:
            raise Exception("Logger is closed")

        if isinstance(message, CloseMessage):
            self._closed = True

        done = False
        while not done:
            try:
                self._queue.put(message, timeout=0.1)
                done = True
            except Full:
                self._logger.warn("Message queue full, trying again")

    def close(self) -> None:
        self.send(CloseMessage())
        self._logger.info("Waiting for message processing to finish")
        self._is_done.wait()
        self._logger.info("Message processing finished")
        self.join()
