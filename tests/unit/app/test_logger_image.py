import pytest
import unittest
from pandas import util

import shutil
from whylogs.app.config import load_config
from whylogs.app.session import session_from_config, get_or_create_session
from whylogs.app.config import SessionConfig, WriterConfig
from freezegun import freeze_time


def test_log_image(tmpdir, image_files):
    output_path = tmpdir.mkdir("whylogs")
    shutil.rmtree(output_path)
    writer_config = WriterConfig("local", ["protobuf"], output_path.realpath())
    yaml_data = writer_config.to_yaml()
    WriterConfig.from_yaml(yaml_data)

    session_config = SessionConfig(
        "project", "pipeline", writers=[writer_config])
    with freeze_time("2015-11-15 04:32:30", tz_offset=-4) as frozen_time:
        session = session_from_config(session_config)

        with session.logger("test", with_rotation_time='s', cache=1) as logger:
            df = util.testing.makeDataFrame()

        for image_file_path in image_files:
            logger.log_image(image_file_path)
    shutil.rmtree(output_path)
