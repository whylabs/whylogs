"""
"""
import datetime
import json
import os
from logging import getLogger
from shutil import rmtree
from time import sleep
from typing import List

import pandas as pd
import pytest

from whylogs import DatasetProfile
from whylogs.app.config import SessionConfig, WriterConfig
from whylogs.app.session import get_or_create_session, session_from_config
from whylogs.app.writers import writer_from_config
from whylogs.util import time

script_dir = os.path.dirname(os.path.realpath(__file__))


def count_features(json_profile_filename):
    if not os.path.isfile(json_profile_filename):
        raise ValueError(f"{json_profile_filename} is not a json file but trying to open it to count features")
    profile = get_json_profile(json_profile_filename)
    if profile and profile.get("columns"):
        return len(profile["columns"].keys())
    return 0


def get_json_profile(json_profile_filename):
    profile = {}
    with open(json_profile_filename) as profile_file:
        profile = json.load(profile_file)
    return profile


def assert_all_elements_equal(data: List):
    if not data or len(data) == 1:
        return True
    first = data[0]
    for element in iter(data):
        assert first[0] == element[0], f"Found differing feature counts: {first[0]} vs {element[0]} in files {first[1]} and {element[1]}"


def test_write_template_path():
    data_time = time.from_utc_ms(9999)
    session_time = time.from_utc_ms(88888)
    path_template = "$name-$session_timestamp-$dataset_timestamp-$session_id"
    writer_config = WriterConfig("local", ["protobuf", "flat"], "output", path_template, "dataset-profile-$name")
    writer = writer_from_config(writer_config)
    dp = DatasetProfile("name", data_time, session_time, session_id="session")
    assert writer.path_suffix(dp) == "name-88888-9999-session"
    assert writer.file_name(dp, ".txt") == "dataset-profile-name.txt"


def test_config_api(tmpdir):
    p = tmpdir.mkdir("whylogs")

    writer_config = WriterConfig("local", ["protobuf", "flat"], p.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])

    session = session_from_config(session_config)

    with session.logger("test_name") as logger:
        logger.log_dataframe(pd.DataFrame())
    session.close()


def test_load_config(tmpdir):
    original_dir = os.curdir

    os.chdir(script_dir)
    p = tmpdir.mkdir("whylogs-config")
    try:
        session = get_or_create_session()
        os.chdir(p)
        assert session.project == "test-project-yaml"

        with session.logger("test_name") as logger:
            logger.log_dataframe(pd.DataFrame())
        session.close()
    finally:
        os.chdir(original_dir)


def test_log_dataframe(tmpdir, df_lending_club):
    p = tmpdir.mkdir("whylogs")

    writer_config = WriterConfig("local", ["protobuf", "flat"], p.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    with session_from_config(session_config) as session:
        with session.logger("lendingclub") as logger:
            assert logger is not None
            logger.log_dataframe(df_lending_club)
            profile = logger.profile
            assert profile is not None

            summary = profile.flat_summary()

            flat_summary = summary["summary"]

            assert len(flat_summary) == 151

    output_files = []
    for root, subdirs, files in os.walk(p):
        output_files += files
    assert len(output_files) == 4


def test_log_csv(tmpdir):
    csv_path = os.path.join(script_dir, os.pardir, "lending_club_1000.csv")
    with get_or_create_session() as session:
        with session.logger("csvtest") as logger:
            logger.log_csv(csv_path)
            summary = logger.profile.flat_summary()
            flat_summary = summary["summary"]

        assert len(flat_summary) == 151


def test_log_multiple_calls(tmpdir, df_lending_club):
    original_dir = os.curdir
    os.chdir(script_dir)

    p = tmpdir.mkdir("whylogs")

    writer_config = WriterConfig("local", ["protobuf", "flat"], p.realpath(), filename_template="dataset_summary-$dataset_timestamp")
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    session = session_from_config(session_config)

    now = datetime.datetime.now()
    for i in range(0, 5):
        with session.logger(dataset_timestamp=now + datetime.timedelta(days=i), with_rotation_time=None) as logger:
            logger.log_dataframe(df_lending_club)
    session.close()

    output_files = []
    for root, subdirs, files in os.walk(p):
        output_files += files
    # we run 5 times, so we should have five times more files than the above test
    assert len(output_files) == 20
    os.chdir(original_dir)


def test_logger_cache(tmpdir):
    from uuid import uuid4

    from whylogs.app import Logger

    logger = Logger(session_id=uuid4(), dataset_name=uuid4())
    logger.log({"name": 1})


@pytest.mark.integtest
def test_log_rotation_concurrency(tmpdir):
    TEST_LOGGER = getLogger(__name__)
    log_rotation_interval = "1s"
    sleep_interval = 2

    test_path = tmpdir.mkdir("log_rotation_concurrency_repro")
    writer_config = WriterConfig("local", ["json"], test_path.realpath(), filename_template="dataset_summary-$dataset_timestamp")

    # Load the full lending club 1000 csv, to get a chance at hitting the bug.
    csv_path = os.path.join(script_dir, os.pardir, "lending_club_1000.csv")
    full_df = pd.read_csv(csv_path)

    # Create a whylogs logging session
    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    session = session_from_config(session_config)

    TEST_LOGGER.info(f"Running rotate log test with {log_rotation_interval} flush intervals and {sleep_interval}s pause")

    with session.logger(tags={"datasetId": "model-1"}, with_rotation_time=log_rotation_interval) as ylog:
        ylog.log_dataframe(full_df)  # Log a larger dataframe to increase chance of rotation before seeing all columns
        sleep(sleep_interval)
        ylog.log_dataframe(full_df.head(n=2))  # Log a smaller dataframe to get more features before rotation
        sleep(sleep_interval)

    output_files = []
    for root, subdir, file_names in os.walk(test_path):
        if not file_names:
            continue
        if subdir:
            for directory in subdir:
                for file in file_names:
                    full_file_path = os.path.join(root, directory, file)
                    output_files += [full_file_path]
        else:
            for file in file_names:
                full_file_path = os.path.join(root, file)
                output_files += [full_file_path]

    assert len(output_files) > 0, "No output files were generated during stress test"
    TEST_LOGGER.debug(f"Generated {len(output_files)} dataset summary files.")

    feature_counts = []
    for filename in output_files:
        feature_count = count_features(filename)
        if feature_count > 0:
            feature_counts.append((count_features(filename), filename))

    assert len(feature_counts) > 0, f"feature counts are all empty, we expect some empty files with aggressive log rotation but not all empty!"
    TEST_LOGGER.info(f"Feature counts all same, first file with features was {feature_counts[0]}")
    TEST_LOGGER.debug(f"There were {len(feature_counts)} files with features.")
    assert_all_elements_equal(feature_counts)
    rmtree(test_path, ignore_errors=True)
    TEST_LOGGER.debug(f"End cleaning up test directory {test_path}")
