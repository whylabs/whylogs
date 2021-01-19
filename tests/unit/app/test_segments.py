import os
import shutil
import pytest
from freezegun import freeze_time
from pandas import util
import datetime
import json
import hashlib

from whylogs.app.config import load_config
from whylogs.app.session import session_from_config, get_or_create_session
from whylogs.app.config import SessionConfig, WriterConfig


def test_segments(df_lending_club, tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig(
        "project", "pipeline", writers=[writer_config])
    session = session_from_config(session_config)
    with session.logger("test", segments=[[{"key": "home_ownership", "value": "RENT"}], [
                                          {"key": "home_ownership", "value": "MORTGAGE"}]], cache=1) as logger:
        logger.log_dataframe(df_lending_club)
        profile = logger.profile
        assert profile is None
        profiles = logger.segmented_profiles
        assert len(profiles) == 2
        assert profiles[list(profiles.keys())[0]].tags["segment"] == json.dumps(
            [{"key": "home_ownership", "value": "RENT"}])
        assert profiles[list(profiles.keys())[1]].tags["segment"] == json.dumps(
            [{"key": "home_ownership", "value": "MORTGAGE"}])
        mortage_segment = logger.get_segment(
            [{"key": "home_ownership", "value": "MORTGAGE"}])
        check_segment = profiles[list(profiles.keys())[1]]
        assert mortage_segment == check_segment
    shutil.rmtree(output_path)


def test_segments_keys(df_lending_club, tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig(
        "project", "pipeline", writers=[writer_config])
    session = session_from_config(session_config)
    with session.logger("test", segments=["emp_title", "home_ownership"],
                        cache=1) as logger:
        logger.log_dataframe(df_lending_club)
        profiles = logger.segmented_profiles
        assert len(profiles) == 47
    shutil.rmtree(output_path)


def test_segments_single_key(df_lending_club, tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig(
        "project", "pipeline", writers=[writer_config])
    session = session_from_config(session_config)
    with session.logger("test", segments=["home_ownership"],
                        cache=1) as logger:
        logger.log_dataframe(df_lending_club)
        profiles = logger.segmented_profiles
        assert len(profiles) == 4

    with session.logger("test2") as logger:
        logger.log_dataframe(df_lending_club, segments=["home_ownership"])
        profiles = logger.segmented_profiles
        assert len(profiles) == 4
    shutil.rmtree(output_path)


def test_segments_with_rotation(df_lending_club, tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig(
        "project", "pipeline", writers=[writer_config])
    with freeze_time("2012-01-14 03:21:34", tz_offset=-4) as frozen_time:
        session = session_from_config(session_config)
        with session.logger("test", with_rotation_time='s', segments=["home_ownership"], profile_full_dataset=True, cache=1) as logger:
            logger.log_dataframe(df_lending_club)
            frozen_time.tick(delta=datetime.timedelta(seconds=1))
            logger.log_dataframe(df_lending_club)
            frozen_time.tick(delta=datetime.timedelta(seconds=1))

            df = util.testing.makeDataFrame()
            with pytest.raises(KeyError):
                logger.log_dataframe(df)
    output_files = []
    for root, subdirs, files in os.walk(output_path):
        output_files += files
    assert len(output_files) == 8
    shutil.rmtree(output_path)

# def test_update_segments(profile_lending_club,tmpdir):
