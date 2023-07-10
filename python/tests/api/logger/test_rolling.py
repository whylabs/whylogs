import functools
import math
import os
from datetime import datetime, timezone
from os import listdir
from os.path import isfile
from typing import Any, Optional, Tuple
from unittest.mock import MagicMock

import pandas as pd
import pytest
import utils as u

import whylogs as why
from whylogs.api.logger.rolling import Scheduler
from whylogs.api.store.local_store import LocalStore
from whylogs.api.store.query import DatasetIdQuery
from whylogs.api.whylabs.session.config import EnvVariableName
from whylogs.api.writer.local import LocalWriter
from whylogs.api.writer.writer import Writable
from whylogs.core.errors import BadConfigError
from whylogs.core.schema import DatasetSchema
from whylogs.core.segmentation_partition import segment_on_column


class TimerContext:
    def __enter__(self) -> "TimerContext":
        self._prev_timer = Scheduler._TIMER
        Scheduler._TIMER = u.FakeTimer
        return self

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        Scheduler._TIMER = self._prev_timer


def use_fake_time(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        with TimerContext():
            return f(*args, **kwargs)

    return wrapper


class TestWriter(LocalWriter):
    """
    The LocalWriter only has second resolution in its filenames, so
    with time faked by the FakeTimer you'd end up with files overwritting
    each other. So this appends a sequence number to the filename to
    prevent collisions.
    """

    __test__ = False  # stop pytest from collecting this class because it starts with Test

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


@pytest.fixture(scope="module", autouse=True)
def set_required_config(request):
    """
    This sets the env that the whylabs writer requries to be set. Typically it would get a hold of it through
    the Session but we haven't made that a mandatory feature.
    """
    os.environ[EnvVariableName.WHYLABS_ORG_ID.value] = "test_org_id"
    os.environ[EnvVariableName.WHYLABS_DEFAULT_DATASET_ID.value] = "dataset-id"
    os.environ[
        EnvVariableName.WHYLABS_API_KEY.value
    ] = "nG6xkOOENZ.Qwx4k4PSsrMgYEV9vZ2BW3ayDE97UiMb94x0YJ5RWVXzowxdPAoS4"  # fake

    def clear_env():
        del os.environ[EnvVariableName.WHYLABS_ORG_ID.value]
        del os.environ[EnvVariableName.WHYLABS_DEFAULT_DATASET_ID.value]
        del os.environ[EnvVariableName.WHYLABS_API_KEY.value]

    request.addfinalizer(clear_env)


@use_fake_time
def test_rolling(tmp_path: Any) -> None:
    """This test is rather unstable so we can only assert the number of files in a range."""

    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    u._FAKE_TIMES = [0, 0.1]  # time list must exist at logger instantiation, but don't fire yet
    u._TIME_INDEX = 0
    with why.logger(
        mode="rolling", interval=1, when="S", base_name="test_base_name", metadata={"foo": "bar"}
    ) as logger:
        assert logger._metadata["foo"] == "bar"
        logger.append_writer(writer=TestWriter(base_dir=tmp_path))
        logger.log(df)
        u._FAKE_TIMES = [
            x / 10.0 for x in range(16)
        ]  # run for 1.5s, might fire 1 or 2 times depending on initial_run_after
        u._TIME_INDEX = 0
        logger._scheduler._timer.start()

        # Note that the number of files generated is depend on the elapsed time
        elapsed = u._FAKE_TIMES[-1]
        assert math.floor(elapsed) <= count_files(tmp_path) <= math.ceil(elapsed)

        u._FAKE_TIMES = [
            1.5 + x / 10.0 for x in range(17)
        ]  # run another 1.5s, fires 1 or 2 times depending on alignment
        u._TIME_INDEX = 0
        logger.log(df)
        logger._scheduler._timer.start()

        elapsed = u._FAKE_TIMES[-1]
        assert math.floor(elapsed) <= count_files(tmp_path) <= math.ceil(elapsed)

        logger.log(df)

    elapsed = u._FAKE_TIMES[-1]
    assert math.floor(elapsed) <= count_files(tmp_path) <= math.ceil(elapsed)


@use_fake_time
def test_rolling_with_callback(tmp_path: Any) -> None:
    rolling_callback = MagicMock()
    messages = [{"col1": i, "col2": i * i * 1.2, "col3": "a"} for i in range(10)]

    u._FAKE_TIMES = [0, 0]
    u._TIME_INDEX = 0
    rolling_logger = why.logger(
        mode="rolling", interval=1, when="S", base_name="test_base_name", callback=rolling_callback
    )
    rolling_logger.append_writer("local", base_dir=tmp_path)
    # process the 10 input messages, and wait a second to allow the rolling logger to hit an interval
    map(rolling_logger.log, messages)
    u._FAKE_TIMES = [0, 1]
    u._TIME_INDEX = 0
    rolling_logger._scheduler._timer.start()

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

    u._FAKE_TIMES = [0, 0]
    u._TIME_INDEX = 0
    with why.logger(mode="rolling", interval=1, when="S", base_name="test_base_name", skip_empty=True) as logger:
        logger.append_writer(writer=TestWriter(base_dir=tmp_path))
        logger.log(df)
        u._FAKE_TIMES = [0, 1, 2.1]
        u._TIME_INDEX = 0
        logger._scheduler._timer.start()

        # Note we sleep for over 2 seconds and have rolling interval of 1 second, so we should see at
        # least two elapsed intervals, but have only one file with skip_empty true
        assert count_files(tmp_path) == 1

        # log one more time, but don't wait and rely on the lifecycle of exiting the above with clause
        # to trigger at least one more flush with some new data
        u._FAKE_TIMES = [2.1, 2.2]
        u._TIME_INDEX = 0
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
        u._FAKE_TIMES = [0, 1.1]  # sleep 1s so it fires once
        u._TIME_INDEX = 0
        logger.log(df)
        logger._scheduler._timer.start()
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
