import os

import boto3
import pytest
from moto import mock_s3

from moto.s3.responses import DEFAULT_REGION_NAME
from smart_open import open

from whylogs.app import WriterConfig
from whylogs.app.config import load_config
from whylogs.app.session import session_from_config
from whylogs.app.writers import writer_from_config

BUCKET = "mocked_bucket"
MY_PREFIX = "mock_folder"

object_keys = [
    "dataset_test_s3/dataset_summary/flat_table/dataset_summary.csv",
    "dataset_test_s3/dataset_summary/frequent_strings/dataset_summary.json",
    "dataset_test_s3/dataset_summary/histogram/dataset_summary.json",
    "dataset_test_s3/dataset_summary/json/dataset_summary.json",
    "dataset_test_s3/dataset_summary/protobuf/dataset_summary.bin",
]

object_keys_meta_config = [
"dataset_test/dataset_summary/protobuf/dataset_summary.bin", 
"metadata/segments.json"
]


MINIO_BUCKET = "minio_bucket"

ENDPOINT_URL = "http://localhost:5000"
AWS_ACCESS_KEY_ID = "minioadmin"
AWS_SECRET_ACCESS_KEY = "minioadmin"




@pytest.fixture
def moto_boto_minio():
    # setup: start moto server and create the bucket
    mocks3 = mock_s3()
    mocks3.start()
    res = boto3.resource(service_name='s3', 
        endpoint_url=ENDPOINT_URL, 
        region_name=DEFAULT_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    res.create_bucket(Bucket=MINIO_BUCKET)

    yield
    # teardown: stop moto server
    mocks3.stop()

@pytest.fixture
def moto_boto():
    # setup: start moto server and create the bucket
    mocks3 = mock_s3()
    mocks3.start()
    res = boto3.resource("s3", region_name=DEFAULT_REGION_NAME)
    res.create_bucket(Bucket=BUCKET)

    yield
    # teardown: stop moto server
    mocks3.stop()


@pytest.mark.usefixtures("moto_boto")
def test_s3_writer_bug(df_lending_club, moto_boto, s3_config_path):

    assert os.path.exists(s3_config_path)

    config = load_config(s3_config_path)
    session = session_from_config(config)

    with session.logger("dataset_test_s3") as logger:
        logger.log_dataframe(df_lending_club)
    session.close()

    client = boto3.client("s3")
    objects = client.list_objects(Bucket="mocked_bucket")

    assert len([each_obj["Key"] for each_obj in objects["Contents"]]) == 1
    assert objects["Contents"][0]["Key"] == "dataset_test_s3/dataset_summary/protobuf/dataset_summary.bin"
    assert "s3:" not in [d.name for d in os.scandir(os.getcwd()) if d.is_dir()]


@pytest.mark.usefixtures("moto_boto")
def test_s3_writer(df_lending_club, moto_boto, s3_all_config_path):

    assert os.path.exists(s3_all_config_path)

    config = load_config(s3_all_config_path)
    session = session_from_config(config)

    with session.logger("dataset_test_s3") as logger:
        logger.log_dataframe(df_lending_club)
    session.close()

    client = boto3.client("s3")
    objects = client.list_objects(Bucket="mocked_bucket")

    for idx, each_objc in enumerate(objects["Contents"]):
        assert each_objc["Key"] == object_keys[idx]


@pytest.mark.usefixtures("moto_boto_minio")
def test_s3_writer_transport(df_lending_club, moto_boto, s3_transport_config_path):

    assert os.path.exists(s3_transport_config_path)
    config = load_config(s3_transport_config_path)

    assert config.writers[0].transport_parameters.endpoint_url == ENDPOINT_URL
    assert config.writers[0].transport_parameters.aws_access_key_id == AWS_ACCESS_KEY_ID
    assert config.writers[0].transport_parameters.aws_secret_access_key == AWS_SECRET_ACCESS_KEY
    assert config.writers[0].transport_parameters.region_name == DEFAULT_REGION_NAME
    # assert config.writers[0].transport_parameters.verify == "path"
    session=session_from_config(config)
    with session.logger("dataset_test_s3") as logger:
        logger.log_dataframe(df_lending_club.head(2))
    session.close()

    # client = boto3.client(service_name="s3",region_name=DEFAULT_REGION_NAME,aws_secret_access_key=AWS_SECRET_ACCESS_KEY,aws_access_key_id=AWS_ACCESS_KEY_ID)

    # objects = client.list_objects(Bucket=MINIO_BUCKET)
    # for idx, each_objc in enumerate(objects["Contents"]):
    #     assert each_objc["Key"] == object_keys[idx]

@pytest.mark.usefixtures("moto_boto")
def test_s3_writer_metadata(df_lending_club, moto_boto, s3_all_config_metadata_path):

    assert os.path.exists(s3_all_config_metadata_path)

    config = load_config(s3_all_config_metadata_path)
    session = session_from_config(config)
    session.estimate_segments(df_lending_club, name="dataset_test", target_field="funded_amnt_inv", max_segments=30)
    client = boto3.client("s3")
    objects = client.list_objects(Bucket="mocked_bucket")

    for idx, each_objc in enumerate(objects["Contents"]):
        assert each_objc["Key"] == "metadata/segments.json"

    with session.logger("dataset_test") as logger:
        logger.log_dataframe(df_lending_club)
    session.close()

    objects = client.list_objects(Bucket="mocked_bucket")
    print(objects)
    for idx, each_objc in enumerate(objects["Contents"]):
        print(each_objc["Key"])
        assert each_objc["Key"] == object_keys_meta_config[idx]


def test_non_valid_type(tmpdir):

    config = WriterConfig(type="blob", formats=["json"], output_path=tmpdir)
    with pytest.raises(ValueError):
        writer_from_config(config)
