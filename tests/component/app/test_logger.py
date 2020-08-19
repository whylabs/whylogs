"""
"""
import os

import pandas as pd

from whylabs.logs.app.config import (
    WriterConfig,
    SessionConfig,
)
from whylabs.logs.app.session import session_from_config, get_or_create_session


def test_config_api(tmpdir):
    p = tmpdir.mkdir("whylogs")

    writer_config = WriterConfig("local", ["protobuf", "csv"], p.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])

    session = session_from_config(session_config)

    with session.logger("test_name") as logger:
        logger.log_dataframe(pd.DataFrame())
    session.close()


def test_load_config(tmpdir):
    original_dir = os.curdir

    p = tmpdir.mkdir("whylogs")

    try:
        session = get_or_create_session()
        os.chdir(p)

        with session.logger("test_name") as logger:
            logger.log_dataframe(pd.DataFrame())
        session.close()
    finally:
        os.chdir(original_dir)


def test_log_dataframe(tmpdir, df_lending_club):
    p = tmpdir.mkdir("whylogs")

    writer_config = WriterConfig("local", ["protobuf", "csv"], p.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", "pipeline", writers=[writer_config])
    session = session_from_config(session_config)

    with session.logger("lendingclub") as logger:
        logger.log_dataframe(df_lending_club)

    output_files = []
    for root, subdirs, files in os.walk(p):
        output_files += files
    assert len(output_files) == 3
