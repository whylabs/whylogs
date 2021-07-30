"""
"""
import datetime
import os

import pandas as pd

from whylogs import DatasetProfile
from whylogs.app.config import SessionConfig, WriterConfig
from whylogs.app.session import get_or_create_session, session_from_config
from whylogs.app.writers import writer_from_config
from whylogs.util import time

script_dir = os.path.dirname(os.path.realpath(__file__))


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
    assert len(output_files) == 5


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
    assert len(output_files) == 25
    os.chdir(original_dir)


def test_logger_cache(tmpdir):
    from uuid import uuid4

    from whylogs.app import Logger

    logger = Logger(session_id=uuid4(), dataset_name=uuid4())
    logger.log({"name": 1})
