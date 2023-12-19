from concurrent.futures import Future
import logging
import multiprocessing as mp
import threading as th
import signal
import sys
from concurrent.futures import Future
from enum import Enum
from typing import Any, Dict, Generic, Optional, Sequence, Tuple, TypeVar

from whylogs.api.logger.experimental.logger.actor.actor import (
    Actor,
    QueueConfig,
    QueueWrapper,
)
from whylogs.api.logger.experimental.logger.actor.future_util import wait_result_while
from whylogs.api.logger.experimental.logger.actor.pipe_signaler import PipeSignaler
from whylogs.api.logger.experimental.logger.actor.process_rolling_logger_messages import (
    ProcessStatusMessage,
    SyncMessage,
)
from whylogs.api.logger.experimental.logger.actor.signal_util import suspended_signals

StatusType = TypeVar("StatusType")
ProcessMessageType = TypeVar("ProcessMessageType")


class QueueType(Enum):
    MP = "MP"
    FASTER_FIFO = "FASTER_FIFO"


class ProcessActor(Actor[ProcessMessageType], mp.Process, Generic[ProcessMessageType, StatusType]):
    """
    Subclass of Actor that uses a process to process messages.
    """

    _wrapper: QueueWrapper[ProcessMessageType]

    def __init__(
        self,
        queue_config: QueueConfig = QueueConfig(),
        queue_type: QueueType = QueueType.FASTER_FIFO,
        sync_enabled: bool = False,
    ) -> None:
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

        self._sync_enabled = sync_enabled
        self._pipe_signaler: Optional[PipeSignaler] = PipeSignaler() if self._sync_enabled is True else None

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

    def _signal(self, messages: Sequence[SyncMessage] = [], error: Optional[Exception] = None) -> None:
        if self._pipe_signaler is None:
            return

        for message in messages:
            if message.sync:
                self._pipe_signaler.signal((message.id, error, None))

    # TODO
    def status(self, timeout: Optional[float] = 1.0) -> StatusType:
        """
        Get the internal status of the Process Actor. Used for diagnostics and debugging.
        This is always synchronous and requires the ProcessActor to be created with sync_enabled=True.
        """
        if self._pipe_signaler is None:
            raise Exception(
                "Can't log synchronously without a pipe signaler. Initialize the process logger with sync_enabled=True."
            )

        # add a sync flag to Message
        message = ProcessStatusMessage()
        future: "Future[StatusType]" = Future()
        self._pipe_signaler.register(future, message.id)
        self.send(message)
        return wait_result_while(future, self.is_alive)

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


class PipeSignaler(th.Thread):
    """
    A thread that listens on a pipe for messages and signals the corresponding futures.

    This class is used in the process actor to enable synchronous requests across processes.
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
        self._parent_conn, self._conn = mp.Pipe()
        self.futures: Dict[str, "Future[Any]"] = {}
        self._end_polling = th.Event()
        self._done = th.Event()

    def signal(self, result: Tuple[str, Optional[Exception], Any]) -> None:
        """
        Signal that a message was handled by sending a tuple of (message id, exception, data).
        data and exception can be None.
        This should be called from the child process.
        """
        self._parent_conn.send(result)

    def register(self, future: "Future[Any]", message_id: str) -> None:
        """
        Register a future to be signaled when the message id is received.
        This should be called from the parent process.
        """
        self._logger.debug(f"Received register request for id {message_id}")
        self.futures[message_id] = future

    def _start_poll_conn(self) -> None:
        while not self._end_polling.is_set():
            try:
                if self._conn.poll(timeout=0.1):
                    message_id, exception, data = self._conn.recv()
                    self._logger.debug(f"Received message id {message_id}")
                    future: Optional["Future[Any]"] = self.futures.pop(message_id, None)
                    if future is not None:
                        self._logger.debug(f"Setting result for message id {message_id} {exception}")
                        if exception is None:
                            print(f"Setting result for message id {message_id} {data}")
                            future.set_result(data)
                        else:
                            future.set_exception(exception)

            except EOFError:
                self._logger.exception("Broken pipe")
                break
            except OSError as e:
                self._logger.exception(f"OS Error in ipc pipe. Was the logger closed? {e}")
            except Exception as e:
                self._logger.exception(f"Error in ipc pipe {e}")

        self._done.set()

    def run(self) -> None:
        self._start_poll_conn()

    def close_child(self) -> None:
        """
        Closes the file descriptors from the child process side.
        """
        self._conn.close()
        self._parent_conn.close()

    def close(self) -> None:
        """
        Closes the thread and all resources. This should be
        called from the parent side.
        """
        self._end_polling.set()
        self._done.wait()

        self._conn.close()
        self._parent_conn.close()

        self.join()
