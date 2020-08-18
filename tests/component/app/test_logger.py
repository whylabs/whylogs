"""
"""
import os

import pandas as pd

from whylabs.logs.app.config import (
    WriterConfig,
    SessionConfig,
)
from whylabs.logs.app.session import session_from_config

MY_DIR = os.path.realpath(os.path.dirname(__file__))
TEST_OUTPUT_DIR = os.path.join(MY_DIR, "test_output", "logger")
TIMESTAMP_MS = 1593710000000
num_failed = 0


def test_load_config(tmpdir):
    p = tmpdir.mkdir("whylogs")

    writer_config = WriterConfig("local", ["protobuf", "flat"], p.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig(
        "project", "pipeline", False, writers=[writer_config]
    )

    session = session_from_config(session_config)

    with session.logger("test_name") as logger:
        logger.log_dataframe(pd.DataFrame())
    session.close()


def test_log_dataframe(tmpdir, df_lending_club):
    p = tmpdir.mkdir("whylogs")

    writer_config = WriterConfig("local", ["protobuf", "flat"], p.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig(
        "project", "pipeline", False, writers=[writer_config]
    )
    session = session_from_config(session_config)

    with session.logger("lendingclub") as logger:
        logger.log_dataframe(df_lending_club)

    output_files = []
    for root, subdirs, files in os.walk(p):
        output_files.append(files)
    assert len(output_files) == 3
