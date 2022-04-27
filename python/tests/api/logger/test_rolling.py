import os
import time
from os import listdir
from os.path import isfile
from typing import Any

import pandas as pd

import whylogs as why


def test_closing(tmp_path: Any, lending_club_df: pd.DataFrame) -> None:
    logger = why.logger(mode="rolling", interval=1, when="H", base_name="test_base_name")
    logger.append_writer("local", base_dir=tmp_path)
    logger.log(lending_club_df)
    logger.close()

    only_files = [f for f in listdir(tmp_path) if isfile(os.path.join(tmp_path, f))]

    assert len(only_files) == 1
    f = only_files[0]
    assert len(only_files) == 1
    assert f.endswith(".bin")
    assert f.startswith("test_base_name")


def test_rolling(tmp_path: Any, lending_club_df: pd.DataFrame) -> None:
    logger = why.logger(mode="rolling", interval=1, when="S", base_name="test_base_name")
    logger.append_writer("local", base_dir=tmp_path)
    logger.log(lending_club_df)
    time.sleep(1.1)
    logger.log(lending_club_df)
    time.sleep(1.1)
    logger.log(lending_club_df)
    logger.close()

    only_files = [f for f in listdir(tmp_path) if isfile(os.path.join(tmp_path, f))]
    assert len(only_files) == 3
