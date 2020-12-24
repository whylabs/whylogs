import pytest
import unittest
from pandas import util 

import time
import os
import shutil
import datetime 
from freezegun import freeze_time

from whylogs.app.config import load_config
from whylogs.app.session import session_from_config
from whylogs.app.config import SessionConfig, WriterConfig


def test_log_rotation_seconds(tmpdir):
    output_path= tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    with freeze_time("2012-01-14 03:21:34", tz_offset=-4) as frozen_time:
            session = session_from_config(session_config)
        with session.logger("test", with_rotation_time='s',cache=1) as logger:
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
            frozen_time.tick(delta=datetime.timedelta(seconds=1))
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
            frozen_time.tick(delta=datetime.timedelta(seconds=1))
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
    output_files = []
    for root, subdirs, files in os.walk(output_path):
        output_files += files
    assert len(output_files) == 3
    shutil.rmtree(output_path)


def test_log_rotation_minutes(tmpdir):
    output_path= tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    with freeze_time("2012-01-14 03:21:34", tz_offset=-4) as frozen_time:
        session = session_from_config(session_config)
        with session.logger("test", with_rotation_time='m',cache=1) as logger:
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
            frozen_time.tick(delta=datetime.timedelta(minutes=2))
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
            frozen_time.tick(delta=datetime.timedelta(minutes=2))
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
    output_files = []
    for root, subdirs, files in os.walk(output_path):
        output_files += files
    assert len(output_files) == 3
    shutil.rmtree(output_path)

def test_log_rotation_days(tmpdir):
    output_path= tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    with freeze_time("2012-01-14 03:21:34", tz_offset=-4) as frozen_time:
        session = session_from_config(session_config)
        with session.logger("test", with_rotation_time='d',cache=1) as logger:
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
            frozen_time.tick(delta=datetime.timedelta(days=1))
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
            frozen_time.tick(delta=datetime.timedelta(days=2))
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
    output_files = []
    for root, subdirs, files in os.walk(output_path):
        output_files += files
    assert len(output_files) == 3
    shutil.rmtree(output_path)

def test_log_rotation_hour(tmpdir):
    output_path= tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    with freeze_time("2012-01-14 03:21:34", tz_offset=-4) as frozen_time:
        session = session_from_config(session_config)
        with session.logger("test", with_rotation_time='h',cache=1) as logger:
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
            frozen_time.tick(delta=datetime.timedelta(hours=3))
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
            df= util.testing.makeDataFrame()
            logger.log_dataframe(df)
            
    output_files = []
    for root, subdirs, files in os.walk(output_path):
        output_files += files
    assert len(output_files) == 2
    shutil.rmtree(output_path)
#
