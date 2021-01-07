

import os
from whylogs.app.session import session_from_config
from whylogs.app.config import load_config
import boto3

from moto import mock_s3
import pytest

BUCKET = "mocked_bucket"
MY_PREFIX = "mock_folder"
# @pytest.fixture(autouse=True)


@pytest.fixture
def moto_boto():
    # setup: start moto server and create the bucket
    mocks3 = mock_s3()
    mocks3.start()
    res = boto3.resource('s3')
    res.create_bucket(Bucket=BUCKET)

    yield
    # teardown: stop moto server
    mocks3.stop()


@pytest.mark.usefixtures("moto_boto")
def test_s3_writer(df_lending_club, moto_boto, s3_config_path):

    assert os.path.exists(s3_config_path)

    config = load_config(s3_config_path)
    session = session_from_config(config)

    with session.logger("dataset_test_s3") as logger:
        logger.log_dataframe(df_lending_club)

    client = boto3.client('s3')
    objects = client.list_objects(Bucket="mocked_bucket")

    assert len([each_obj["Key"] for each_obj in objects["Contents"]]) == 1
    assert objects["Contents"][0]["Key"] == "dataset_test_s3/dataset_summary/protobuf/dataset_summary.bin"
    assert "s3:" not in [d.name for d in os.scandir(
        os.getcwd()) if d.is_dir()]
