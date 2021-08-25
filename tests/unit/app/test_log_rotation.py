import datetime
import os
import shutil
from datetime import timezone

import pytest
from freezegun import freeze_time
from pandas import util

from whylogs.app.config import SessionConfig, WriterConfig
from whylogs.app.logger import Logger
from whylogs.app.session import get_or_create_session, session_from_config
from whylogs.app.writers import writer_from_config


def test_no_log_rotation(tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    basewriter = writer_from_config(WriterConfig.from_yaml(yaml_data))
    l = Logger(session_id="", dataset_name="testing", writers=[basewriter], dataset_timestamp=datetime.datetime.now(tz=timezone.utc), with_rotation_time=None)
    l.log({"quick_test": 3})
    l.flush()


def test_log_rotation_parsing():
    with freeze_time("2012-01-14 03:21:34", tz_offset=-4) as frozen_time:
        l = Logger(session_id="", dataset_name="testing")
        now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        l._set_rotation(with_rotation_time="s")
        assert l.interval == 1
        assert l.rotate_at == now + 1
        l._set_rotation(with_rotation_time="m")
        assert l.interval == 60
        assert l.rotate_at == (now + l.interval)
        l._set_rotation(with_rotation_time="h")
        assert l.interval == 60 * 60
        assert l.rotate_at == (now + l.interval)
        l._set_rotation(with_rotation_time="d")
        assert l.interval == 24 * 60 * 60
        assert l.rotate_at == now + l.interval
        l._set_rotation(with_rotation_time="30s")
        assert l.interval == 30
        assert l.rotate_at == now + l.interval
        l._set_rotation(with_rotation_time="10m")
        assert l.interval == 10 * 60
        assert l.rotate_at == now + l.interval
        l._set_rotation(with_rotation_time="10h")
        assert l.interval == 10 * 60 * 60
        assert l.rotate_at == now + l.interval
        l._set_rotation(with_rotation_time="2d")
        assert l.interval == 2 * 24 * 60 * 60
        assert l.rotate_at == now + l.interval
        # make sure bogus specifications get flagged.
        with pytest.raises(TypeError):
            l._set_rotation(with_rotation_time="-2d")
        with pytest.raises(TypeError):
            l._set_rotation(with_rotation_time="2")
        with pytest.raises(TypeError):
            l._set_rotation(with_rotation_time="s2")
        l.close()

def test_log_rotation_seconds(tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    with freeze_time("2012-01-14 03:21:34", tz_offset=-4) as frozen_time:
        with session_from_config(session_config) as session:
            with session.logger("test", with_rotation_time="s", cache_size=1) as logger:
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
                frozen_time.tick(delta=datetime.timedelta(seconds=1))
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
                frozen_time.tick(delta=datetime.timedelta(seconds=1))
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
                logger.close()
    output_files = []
    for root, subdirs, files in os.walk(output_path):
        output_files += files
    assert len(output_files) == 3
    shutil.rmtree(output_path, ignore_errors=True)


def test_log_rotation_minutes(tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    with freeze_time("2012-01-14 03:21:34", tz_offset=-4) as frozen_time:
        with session_from_config(session_config) as session:
            with session.logger("test", with_rotation_time="m", cache_size=1) as logger:
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
                frozen_time.tick(delta=datetime.timedelta(minutes=2))
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
                frozen_time.tick(delta=datetime.timedelta(minutes=2))
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
                logger.close()
    output_files = []
    for root, subdirs, files in os.walk(output_path):
        output_files += files
    assert len(output_files) == 3
    shutil.rmtree(output_path, ignore_errors=True)


def test_log_rotation_days(tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    with freeze_time("2012-01-14 03:21:34", tz_offset=-4) as frozen_time:
        with session_from_config(session_config) as session:
            with session.logger("test", with_rotation_time="d", cache_size=1) as logger:
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
                frozen_time.tick(delta=datetime.timedelta(days=1))
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
                frozen_time.tick(delta=datetime.timedelta(days=2))
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
    output_files = []
    for root, subdirs, files in os.walk(output_path):
        output_files += files
    assert len(output_files) == 3
    shutil.rmtree(output_path, ignore_errors=True)


def test_log_rotation_hour(tmpdir):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path, ignore_errors=True)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    with freeze_time("2012-01-14 03:21:34", tz_offset=-4) as frozen_time:
        with session_from_config(session_config) as session:
            with session.logger("test", with_rotation_time="h", cache_size=1) as logger:
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
                frozen_time.tick(delta=datetime.timedelta(hours=3))
                logger.log(feature_name="E", value=4)
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)

    output_files = []
    for root, subdirs, files in os.walk(output_path):
        output_files += files
    assert len(output_files) == 2
    shutil.rmtree(output_path, ignore_errors=True)


def test_incorrect_rotation_time():

    with pytest.raises(TypeError):
        with get_or_create_session() as session:
            with session.logger("test2", with_rotation_time="W2") as logger:
                df = util.testing.makeDataFrame()
                logger.log_dataframe(df)
