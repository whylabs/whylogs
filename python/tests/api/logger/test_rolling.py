import math
import os
import time
from datetime import datetime, timezone
from os import listdir
from os.path import isfile
from threading import Timer
from typing import Any, Callable, List, Optional, Tuple
from unittest.mock import MagicMock

import pandas as pd
import pytest

import whylogs as why
from whylogs.api.logger.rolling import Scheduler
from whylogs.api.store.local_store import LocalStore
from whylogs.api.store.query import DatasetIdQuery
from whylogs.api.writer.local import LocalWriter
from whylogs.api.writer.writer import Writable
from whylogs.core.errors import BadConfigError
from whylogs.core.schema import DatasetSchema
from whylogs.core.segmentation_partition import segment_on_column


# A sequence of timers get instantiated, so we need global state for them
# to simulate universal time.

_FAKE_TIMES: List[float] = []  # list of times (seconds) where the FakeTimer checks if it should execute
_TIME_INDEX: int = 0  # where are we in the lsit of times


class FakeTimer:
    """
    Fake for threading.Timer, not threaded. Evaluates elapsed duration
    at times in the _FAKE_TIME list.
    """
    def __init__(self, interval: float, fn: Callable) -> None:
        self._interval = interval
        self._fn = fn
        self._prev_start_time = 0.0
        if _TIME_INDEX < len(_FAKE_TIMES):
            self._prev_start_time = _FAKE_TIMES[_TIME_INDEX]

    def start(self) -> None:
        global _TIME_INDEX
        _TIME_INDEX += 1
        if _TIME_INDEX >= len(_FAKE_TIMES):
            return

        now = _FAKE_TIMES[_TIME_INDEX]
        if (now - self._prev_start_time) >= self._interval:
            self._fn()
            self._prev_start_time = now
        else:
            self.start()

    def cancel(self) -> None:
        global _TIME_INDEX
        _TIME_INDEX = len(_FAKE_TIMES) + 1


class TimerConext:
    def __enter__(self) -> "TimerContext":
        self._prev_timer = Scheduler._TIMER
        Scheduler._TIMER = FakeTimer
        return self

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        Scheduler._TIMER = self._prev_timer


import functools
def use_fake_time(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        with TimerConext():
            return f(*args, **kwargs)

    return wrapper


class TestWriter(LocalWriter):
    """
    The LocalWriter only has second resolution in its filenames, so
    with time faked by the FakeTimer you'd end up with files overwritting
    each other. So this appends a sequence number to the filename to
    prevent collisions.
    """
    def __init__(self, base_dir: Optional[str] = None, base_name: Optional[str] = None) -> None:
        super().__init__(base_dir, base_name)
        self._counter = 1

    def write(
        self,
        file: Writable,
        dest: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bool, str]:
        dest = dest or self._base_name or file.get_default_path()  # type: ignore
        dest += str(self._counter)
        self._counter += 1
        full_path = os.path.join(self._base_dir, dest)
        file.write(full_path, **kwargs)
        return True, full_path




def test_closing(tmp_path: Any, lending_club_df: pd.DataFrame) -> None:
    with why.logger(mode="rolling", interval=1, when="H", base_name="test_base_name") as logger:
        logger.append_writer("local", base_dir=tmp_path)
        logger.log(lending_club_df)

    only_files = [f for f in listdir(tmp_path) if isfile(os.path.join(tmp_path, f))]

    assert len(only_files) == 1
    f = only_files[0]
    assert len(only_files) == 1
    assert f.endswith(".bin")
    assert f.startswith("test_base_name")


@use_fake_time
def test_rolling(tmp_path: Any) -> None:
    """This test is rather unstable so we can only assert the number of files in a range."""

    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    global _FAKE_TIMES, _TIME_INDEX
    _FAKE_TIMES = [0, 0.1]
    _TIME_INDEX = 0

    # start = time.time()
    with why.logger(mode="rolling", interval=1, when="S", base_name="test_base_name") as logger:
        logger.append_writer(writer=TestWriter(base_dir=tmp_path))
        logger.log(df)
        _FAKE_TIMES = [x / 10.0 for x in range(16)]
        _TIME_INDEX = 0
        logger._scheduler._timer.start()
        # time.sleep(1.5)

        # Note that the number of files generated is depend on the elapsed amount
        elapsed = _FAKE_TIMES[-1]
        # elapsed = time.time() - start

        assert math.floor(elapsed) <= count_files(tmp_path) <= math.ceil(elapsed)

        _FAKE_TIMES = [1.5 + x / 10.0 for x in range(17)]
        _TIME_INDEX = 0
        logger.log(df)
        logger._scheduler._timer.start()
        # time.sleep(1.5)
        elapsed = _FAKE_TIMES[-1]
        # elapsed = time.time() - start
        assert math.floor(elapsed) <= count_files(tmp_path) <= math.ceil(elapsed)

        logger.log(df)

    elapsed = _FAKE_TIMES[-1]
    # elapsed = time.time() - start
    assert math.floor(elapsed) <= count_files(tmp_path) <= math.ceil(elapsed)


@use_fake_time
def test_rolling_with_callback(tmp_path: Any) -> None:
    rolling_callback = MagicMock()
    messages = [{"col1": i, "col2": i * i * 1.2, "col3": "a"} for i in range(10)]

    global _FAKE_TIMES, _TIME_INDEX
    _FAKE_TIMES = [0, 0]
    _TIME_INDEX = 0
    rolling_logger = why.logger(
        mode="rolling", interval=1, when="S", base_name="test_base_name", callback=rolling_callback
    )
    rolling_logger.append_writer("local", base_dir=tmp_path)
    # process the 10 input messages, and wait a second to allow the rolling logger to hit an interval
    map(rolling_logger.log, messages)
    _FAKE_TIMES = [0, 1]
    _TIME_INDEX = 0
    rolling_logger._scheduler._timer.start()
    # time.sleep(1)

    # without an explicit calls to rolling_logger.flush we expect that the elapsed time
    # is greater than 1s interval, so the callback should have been triggered at least once
    initial_callback_count = rolling_callback.call_count
    assert initial_callback_count > 0
    assert initial_callback_count < 4

    # after explicitly calling close on the logger, we trigger at least one more flush and callback
    rolling_logger.close()
    assert initial_callback_count < rolling_callback.call_count


@use_fake_time
def test_rolling_skip_empty(tmp_path: Any) -> None:
    """This test is rather unstable so we can only assert the number of files in a range."""

    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    global _FAKE_TIMES, _TIME_INDEX
    _FAKE_TIMES = [0, 0]
    _TIME_INDEX = 0
    with why.logger(mode="rolling", interval=1, when="S", base_name="test_base_name", skip_empty=True) as logger:
        logger.append_writer(writer=TestWriter(base_dir=tmp_path))
        logger.log(df)
        _FAKE_TIMES = [0, 1, 2.1]
        _TIME_INDEX = 0
        logger._scheduler._timer.start()
        # time.sleep(2.1)

        # Note we sleep for over 2 seconds and have rolling interval of 1 second, so we should see at
        # least two elapsed intervals, but have only one file with skip_empty true
        assert count_files(tmp_path) == 1

        # log one more time, but don't wait and rely on the lifecycle of exiting the above with clause
        # to trigger at least one more flush with some new data
        _FAKE_TIMES = [2.1, 2.2]
        _TIME_INDEX = 0
        logger.log(df)
        logger._scheduler._timer.start()
    assert count_files(tmp_path) == 2


def test_bad_whylabs_writer_config() -> None:
    with pytest.raises(BadConfigError) as excinfo:
        with why.logger(mode="rolling", interval=1, when="S", base_name="test_base_name", skip_empty=True) as logger:
            logger.append_writer("whylabs")
        assert "Bad WhyLabsWriter config" in str(excinfo.value)
        assert "five minutes" in str(excinfo.value)


def test_good_whylabs_writer_config() -> None:
    with why.logger(mode="rolling", interval=5, when="M", base_name="test_base_name", skip_empty=True) as logger:
        logger.append_writer("whylabs")


def count_files(tmp_path: Any) -> int:
    only_files = [f for f in listdir(tmp_path) if isfile(os.path.join(tmp_path, f))]
    return len(only_files)


@use_fake_time
def test_rolling_with_local_store_writes() -> None:
    store = LocalStore()
    df = pd.DataFrame(data={"a": [1, 2, 3, 4]})

    with why.logger(mode="rolling", interval=1, when="S", base_name="test_base_name", skip_empty=True) as logger:
        logger.append_store(store=store)
        global _FAKE_TIMES, _TIME_INDEX
        _FAKE_TIMES = [0, 1.1]
        _TIME_INDEX = 0
        logger.log(df)
        logger._scheduler._timer.start()
        # time.sleep(1)
        assert len(store.list()) == 1

        query = DatasetIdQuery(dataset_id="test_base_name")
        assert store.get(query=query)


def test_rolling_row_messages_with_segments(tmp_path: Any) -> None:
    rolling_callback = MagicMock()
    segment_column = "col1"
    messages = [{"col1": i % 2, "col2": i * i * 1.2, "col3": "a"} for i in range(10)]

    segment_schema = DatasetSchema(segments=segment_on_column(segment_column))

    rolling_logger = why.logger(
        schema=segment_schema,
        mode="rolling",
        interval=60,
        when="S",
        base_name="test_base_name",
        callback=rolling_callback,
    )
    rolling_logger.append_writer("local", base_dir=tmp_path)
    # process the 10 input messages, and wait a second to allow the rolling logger to hit an interval
    for message in messages:
        rolling_logger.log(message)

    # without an explicit call to rolling_logger.flush we expect that the elapsed time
    # is less than the 60s interval, so the callback should not have been triggered
    assert rolling_callback.call_count == 0

    # after explicitly calling close on the logger, we trigger one more flush which has two segments and expect two callbacks
    rolling_logger.close()
    assert rolling_callback.call_count == 2


def test_rolling_do_rollover():
    import pandas as pd

    df = pd.DataFrame(data={"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]})
    rolling_logger = why.logger(mode="rolling", interval=5, when="M", base_name="profile_")
    rolling_logger.append_writer("local")
    rolling_logger.log(df)
    now = datetime.now(timezone.utc)
    initial_profile_id = id(rolling_logger._current_profile)
    profile_timestamp = rolling_logger._current_profile.dataset_timestamp
    rolling_logger._do_rollover()
    post_rollover_profile_id = id(rolling_logger._current_profile)
    rolling_logger.close()
    assert initial_profile_id != post_rollover_profile_id
    assert now.timestamp() == pytest.approx(profile_timestamp.timestamp())
    # these lines below fail as a comparison
    # now_plus_1h = now + datetime.timedelta(hours=1)
    # assert now.timestamp() == pytest.approx(now_plus_1h.timestamp())
