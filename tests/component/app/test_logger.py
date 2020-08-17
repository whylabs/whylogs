"""
"""
from whylabs.logs import get_logger, get_or_create_session
from whylabs.logs.app.config import (
    WriterConfig,
    WriterConfigSchema,
    SessionConfig,
    load_config,
)
from whylabs.logs.app.session import reset_default, Session
from whylabs.logs.app.writers import Writer
from whylabs.logs.core import DatasetProfile

import shutil
import pandas as pd
import os
import json

MY_DIR = os.path.realpath(os.path.dirname(__file__))
TEST_OUTPUT_DIR = os.path.join(MY_DIR, "test_output", "logger")
TIMESTAMP_MS = 1593710000000
num_failed = 0


def test_load_config():
    writer_config = WriterConfig("s3", ["protobuf", "flat"], "s3://bucket/key")
    print("foo")
    yaml_data = writer_config.to_yaml()
    print(yaml_data)
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig("project", False, writers=[writer_config])
    print(session_config.to_yaml())
    print(load_config().to_yaml())

    session = Session(
        session_config.project,
        writers=[Writer("s3", "s3:/abc/xyz", "s3:/acx", list("csv"))],
        verbose=False,
    )
    with session.logger("test_name") as logger:
        logger.log_dataframe(pd.DataFrame())
    session.close()


def test_log_dataframe(df_lending_club):
    reset_default()
    get_or_create_session(
        output_to_stdout=False,
        output_to_cloud=False,
        local_output_folder=TEST_OUTPUT_DIR,
        pipeline="pipeline_name",
        team="team_name",
    )
    logger = get_logger()
    response = logger.log_dataframe(df_lending_club, "lending_club")
    rs = response["handler_responses"]
    assert len(rs) == 3
    expected_outputs = {
        ("protobuf", "disk"),
        ("flat", "disk"),
        ("json", "disk"),
    }
    outputs = set()
    for r in rs:
        handler_response = r["response"]
        if r["fmt"] == "protobuf" and r["dest"] == "disk":
            outputs.add(("protobuf", "disk"))
            # Verify that we can load the protobuf
            with open(handler_response.dest, "rb") as fp:
                DatasetProfile.from_protobuf_string(fp.read())
        elif r["fmt"] == "flat" and r["dest"] == "disk":
            outputs.add(("flat", "disk"))
            # Verify we can load all the flat datas
            pd.read_csv(handler_response.dest["flat_table"])
            json.load(open(handler_response.dest["histogram"], "rt"))
            json.load(open(handler_response.dest["freq_strings"], "rt"))
        elif r["fmt"] == "json" and r["dest"] == "disk":
            outputs.add(("json", "disk"))
            # Verify we can load the JSON summary
            json.load(open(handler_response.dest, "rt"))

    assert outputs == expected_outputs

    shutil.rmtree(TEST_OUTPUT_DIR)
