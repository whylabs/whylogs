import multiprocessing as mp
import os
from dataclasses import dataclass
from typing import Any, List, Type, Union

import pytest

from whylogs.api.logger.experimental.logger.actor.actor import CloseMessage, QueueConfig
from whylogs.api.logger.experimental.logger.actor.process_actor import (
    ProcessActor,
    QueueType,
)
from whylogs.api.logger.experimental.logger.actor.process_rolling_logger_messages import (
    ProcessStatusMessage,
)
from whylogs.api.logger.experimental.logger.actor.thread_actor import ThreadActor


@dataclass
class Message1:
    pass


@dataclass
class Message2:
    pass


Messages = Union[Message1, Message2]


class Counter:
    def __init__(self) -> None:
        self.m1 = mp.Value("i", 0)
        self.m2 = mp.Value("i", 0)
        self.total = mp.Value("i", 0)
        self.call_count = mp.Value("i", 0)

    def process_batch(
        self, batch: List[Any], batch_type: Type[Union[Messages, ProcessStatusMessage, CloseMessage]]
    ) -> None:
        with self.call_count.get_lock():
            self.call_count.value += 1  # type: ignore

        if batch_type == Message1:
            with self.m1.get_lock():
                self.m1.value += len(batch)  # type: ignore
        elif batch_type == Message2:
            with self.m2.get_lock():
                self.m2.value += len(batch)  # type: ignore
        elif batch_type == CloseMessage:
            with self.total.get_lock(), self.m1.get_lock(), self.m2.get_lock():
                self.total.value = self.m1.value + self.m2.value  # type: ignore
        else:
            raise Exception(f"Unknown batch type: {batch_type}")


class CountingMPProcessActor(ProcessActor[Messages, Any]):
    def __init__(self, queue_config: QueueConfig = QueueConfig()) -> None:
        super().__init__(queue_config, queue_type=QueueType.MP)
        self.counter = Counter()

    def process_batch(self, batch, batch_type) -> None:
        self.counter.process_batch(batch, batch_type)


class CountingFasterFifoProcessActor(ProcessActor[Messages, Any]):
    def __init__(self, queue_config: QueueConfig = QueueConfig()) -> None:
        super().__init__(queue_config, queue_type=QueueType.FASTER_FIFO)
        self.counter = Counter()

    def process_batch(self, batch, batch_type) -> None:
        self.counter.process_batch(batch, batch_type)


class CountingThreadActor(ThreadActor[Messages]):
    def __init__(self, queue_config: QueueConfig = QueueConfig()) -> None:
        super().__init__(queue_config)
        self.counter = Counter()

    def process_batch(
        self, batch: List[Union[Messages, CloseMessage]], batch_type: Type[Union[Messages, CloseMessage]]
    ) -> None:
        self.counter.process_batch(batch, batch_type)


ActorTypes = Union[Type[CountingMPProcessActor], Type[CountingThreadActor], Type[CountingFasterFifoProcessActor]]

actors: List[ActorTypes] = [CountingMPProcessActor, CountingThreadActor]
if os.name != "nt":
    actors.append(CountingFasterFifoProcessActor)


@pytest.mark.parametrize("Act", actors)
def test_process_actor_happy_path(Act: ActorTypes) -> None:
    actor = Act()  # type: ignore
    if isinstance(actor, ProcessActor):
        actor.start()

    actor.send(Message1())
    actor.send(Message1())
    actor.send(Message1())

    actor.send(Message2())
    actor.send(Message2())

    actor.close()

    with actor.counter.call_count.get_lock():
        # 1 for the Message1 block, 1 for the Message2 block, 1 for the CloseMessage
        assert actor.counter.call_count.value == 3  # type: ignore
    with actor.counter.total.get_lock(), actor.counter.m1.get_lock(), actor.counter.m2.get_lock():
        assert actor.counter.total.value == 5  # type: ignore
        assert actor.counter.m1.value == 3  # type: ignore
        assert actor.counter.m2.value == 2  # type: ignore


@pytest.mark.parametrize("Act", actors)
def test_process_actor_sparse_batches(Act: Union[Type[CountingMPProcessActor], Type[CountingThreadActor]]) -> None:
    config = QueueConfig(max_batch_size=1)
    actor = Act(config)
    if isinstance(actor, ProcessActor):
        actor.start()

    actor.send(Message1())
    actor.send(Message1())
    actor.send(Message1())

    actor.send(Message2())
    actor.send(Message2())

    actor.close()

    with actor.counter.call_count.get_lock():
        # 1 for the Message1 block, 1 for the Message2 block, 1 for the CloseMessage
        assert actor.counter.call_count.value == 6  # type: ignore
    with actor.counter.total.get_lock(), actor.counter.m1.get_lock(), actor.counter.m2.get_lock():
        assert actor.counter.total.value == 5  # type: ignore
        assert actor.counter.m1.value == 3  # type: ignore
        assert actor.counter.m2.value == 2  # type: ignore


@pytest.mark.parametrize("Act", actors)
def test_large_batches(Act: Union[Type[CountingMPProcessActor], Type[CountingThreadActor]]) -> None:
    config = QueueConfig(max_batch_size=100)
    actor = Act(config)
    if isinstance(actor, ProcessActor):
        actor.start()

    actor.send(Message1())
    actor.send(Message1())
    actor.send(Message1())

    actor.send(Message2())
    actor.send(Message2())

    actor.close()

    with actor.counter.call_count.get_lock():
        # 1 for the Message1 block, 1 for the Message2 block, 1 for the CloseMessage
        assert actor.counter.call_count.value == 3  # type: ignore

    with actor.counter.total.get_lock(), actor.counter.m1.get_lock(), actor.counter.m2.get_lock():
        assert actor.counter.total.value == 5  # type: ignore
        assert actor.counter.m1.value == 3  # type: ignore
        assert actor.counter.m2.value == 2  # type: ignore
