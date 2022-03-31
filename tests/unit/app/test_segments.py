import datetime
import os
import pandas as pd
import pytest
import shutil
from freezegun import freeze_time
from logging import getLogger

from whylogs.app.config import SessionConfig, WriterConfig
from whylogs.app.logger import _TAG_PREFIX, _TAG_VALUE
from whylogs.app.session import session_from_config

TEST_LOGGER = getLogger(__name__)

def test_segments(df_lending_club, tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)
    test_segments = [
        [{"key": "home_ownership", "value": "RENT"}],
        [{"key": "home_ownership", "value": "MORTGAGE"}],
    ]

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    with session_from_config(session_config) as session:
        with session.logger(
            "test",
            segments=test_segments,
            cache_size=1,
        ) as logger:
            logger.log_dataframe(df_lending_club)
            profile = logger.profile
            profiles = logger.segmented_profiles
            mortage_segment = logger.get_segment(test_segments[1])

    assert profile is None
    assert len(profiles) == 2
    segment_keys = [key for key in profiles[list(profiles.keys())[0]].tags.keys() if key.startswith(_TAG_PREFIX)]
    for segment_key in segment_keys:
        assert profiles[list(profiles.keys())[0]].tags[segment_key] == test_segments[0][0][_TAG_VALUE]  # 'RENT'

    segment_keys = [key for key in profiles[list(profiles.keys())[1]].tags.keys() if key.startswith(_TAG_PREFIX)]
    for segment_key in segment_keys:
        assert profiles[list(profiles.keys())[1]].tags[segment_key] == test_segments[1][0][_TAG_VALUE]  # 'MORTGAGE'

    check_segment = profiles[list(profiles.keys())[1]]
    assert mortage_segment == check_segment
    shutil.rmtree(output_path, ignore_errors=True)


def test_segments_keys(df_lending_club, tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    session = session_from_config(session_config)
    with session.logger("test", segments=["emp_title", "home_ownership"], cache_size=1) as logger:
        logger.log_dataframe(df_lending_club)
        profiles = logger.segmented_profiles
        for _, prof in profiles.items():
            TEST_LOGGER.info(prof.tags)
        assert len(profiles) == 47
    shutil.rmtree(output_path, ignore_errors=True)


def test_segments_single_key(df_lending_club, tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    session = session_from_config(session_config)
    home_ownership_values = set(df_lending_club["home_ownership"])
    TEST_LOGGER.info(f"Unique home_ownership values are: {home_ownership_values}")
    with session.logger("test", segments=["home_ownership"], cache_size=1) as logger:
        logger.log_dataframe(df_lending_club)
        
        profiles1 = logger.segmented_profiles
        assert len(profiles1) == len(home_ownership_values)
        for _, prof in profiles1.items():
            assert prof.tags['whylogs.tag.home_ownership'] in home_ownership_values
            
    with session.logger("test2") as logger:
        logger.log_dataframe(df_lending_club, segments=["home_ownership"])
        profiles2 = logger.segmented_profiles
        TEST_LOGGER.info(profiles2)
    session.close()
    assert len(profiles1) == 4
    assert len(profiles2) == 4
    shutil.rmtree(output_path, ignore_errors=True)


def test_segments_with_rotation(df_lending_club, df, tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    with freeze_time("2012-01-14 03:21:34", tz_offset=-4) as frozen_time:
        session = session_from_config(session_config)
        with session.logger(
            "test",
            with_rotation_time="s",
            segments=["home_ownership"],
            profile_full_dataset=True,
            cache_size=1,
        ) as logger:
            logger.log_dataframe(df_lending_club)
            frozen_time.tick(delta=datetime.timedelta(seconds=1))
            logger.log_dataframe(df_lending_club)
            frozen_time.tick(delta=datetime.timedelta(seconds=1))

            with pytest.raises(KeyError):
                logger.log_dataframe(df)
        session.close()
    output_files = []
    for _, _, files in os.walk(output_path):
        output_files += files
    assert len(output_files) == 8
    shutil.rmtree(output_path, ignore_errors=True)


def test_one_segment(tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])

    session = session_from_config(session_config)

    df = pd.DataFrame(data={"x": [1], "y": [4], "z": [0.1]})
    with session.logger("segment_test", segments=["x", "y"]) as logger:
        logger.log_segments(df)
        assert len(logger.segmented_profiles) == 1


def test_log_multiple_segments(tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])

    session = session_from_config(session_config)

    df = pd.DataFrame(data={"x": [1, 2, 3, 1, 2, 3, 1, 2, 3], "y": [4, 5, 6, 5, 6, 4, 6, 4, 5], "z": [0.1, 0.2, 0.3, 0.1, 0.2, 0.3, 0.1, 0.2, 0.3]})
    with session.logger("image_test", segments=["x", "y"]) as logger:
        logger.log_segments(df)
        assert len(logger.segmented_profiles) == 9
