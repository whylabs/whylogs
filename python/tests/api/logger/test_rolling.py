import math
import os
import time
from os import listdir
from os.path import isfile
from typing import Any

import pandas as pd
import pytest

import whylogs as why
from whylogs.core.errors import BadConfigError


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


def test_rolling(tmp_path: Any) -> None:
    """This test is rather unstable so we can only assert the number of files in a range."""

    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    start = time.time()
    with why.logger(mode="rolling", interval=1, when="S", base_name="test_base_name") as logger:
        logger.append_writer("local", base_dir=tmp_path)
        logger.log(df)
        time.sleep(1.5)

        # Note that the number of files generated is depend on the elapsed amount
        elapsed = time.time() - start
        assert math.floor(elapsed) <= count_files(tmp_path) <= math.ceil(elapsed)

        logger.log(df)
        time.sleep(1.5)
        elapsed = time.time() - start
        assert math.floor(elapsed) <= count_files(tmp_path) <= math.ceil(elapsed)

        logger.log(df)

    elapsed = time.time() - start
    assert math.floor(elapsed) <= count_files(tmp_path) <= math.ceil(elapsed)


def test_rolling_skip_empty(tmp_path: Any) -> None:
    """This test is rather unstable so we can only assert the number of files in a range."""

    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    with why.logger(mode="rolling", interval=1, when="S", base_name="test_base_name", skip_empty=True) as logger:
        logger.append_writer("local", base_dir=tmp_path)
        logger.log(df)
        time.sleep(2)

        # Note that the number of files generated is depend on the elapsed amount
        assert count_files(tmp_path) == 1

        logger.log(df)
        time.sleep(2)
        assert count_files(tmp_path) == 2

        logger.log(df)

    assert count_files(tmp_path) == 3


def test_bad_whylabs_writer_config() -> None:
    with pytest.raises(BadConfigError) as excinfo:
        with why.logger(mode="rolling", interval=1, when="S", base_name="test_base_name", skip_empty=True) as logger:
            logger.append_writer("whylabs")
            pass
        assert "Bad WhyLabsWriter config" in str(excinfo.value)
        assert "five minutes" in str(excinfo.value)


def test_good_whylabs_writer_config() -> None:
    with why.logger(mode="rolling", interval=5, when="M", base_name="test_base_name", skip_empty=True) as logger:
        logger.append_writer("whylabs")
        pass


def count_files(tmp_path: Any) -> int:
    only_files = [f for f in listdir(tmp_path) if isfile(os.path.join(tmp_path, f))]
    return len(only_files)
