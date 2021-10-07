import cProfile
import json
import os
import pstats
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
TEST_LOGGER = getLogger(__name__)


def count_features(json_profile_filename):
    if not os.path.isfile(json_profile_filename):
        raise ValueError(f"{json_profile_filename} is not a json file but trying to open it to count features")
    profile = get_json_profile(json_profile_filename)
    if profile and profile.get("columns"):
        return len(profile["columns"].keys())
    return 0


def get_json_profile(json_profile_filename):
    profile = {}
    if os.path.exists(json_profile_filename) and os.stat(json_profile_filename).st_size > 0:
        with open(json_profile_filename) as profile_file:
            profile = json.load(profile_file)
    return profile


def assert_all_elements_equal(data: List):
    if not data or len(data) == 1:
        return True
    first = data[0]
    for element in iter(data):
        assert first[0] == element[0], f"Found differing feature counts: {first[0]} vs {element[0]} in files {first[1]} and {element[1]}"


@pytest.mark.load
def test_log_rotation_concurrency(tmpdir):
    log_rotation_interval = "1s"
    sleep_interval = 2

    test_path = tmpdir.mkdir("log_rotation_concurrency_repro")
    writer_config = WriterConfig("local", ["json"], test_path.realpath(), filename_template="dataset_summary-$dataset_timestamp")

    # Load the full lending club 1000 csv, to get a chance at hitting the bug.
    csv_path = os.path.join(script_dir, "lending_club_1000.csv")
    full_df = pd.read_csv(csv_path)

    # full_df has shape (1000, 151) so create a test df with 4x size by iteratively appending to self 2 times
    for _ in range(2):
        full_df = full_df.append(full_df)

    TEST_LOGGER.info(f"test dataframe has shape {full_df.shape}")

    # Create a whylogs logging session
    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    session = session_from_config(session_config)

    TEST_LOGGER.info(f"Running rotate log test with {log_rotation_interval} flush intervals and {sleep_interval}s pause")
    profiler = cProfile.Profile()
    profiler.enable()
    with session.logger(tags={"datasetId": "model-1"}, with_rotation_time=log_rotation_interval) as ylog:
        ylog.log_dataframe(full_df)  # Log a larger dataframe to increase chance of rotation before seeing all columns
        sleep(sleep_interval)
        ylog.log_dataframe(full_df.head(n=2))  # Log a smaller dataframe to get more features before rotation
        sleep(sleep_interval)
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats("cumulative")
    TEST_LOGGER.info(stats.print_stats(10))

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
