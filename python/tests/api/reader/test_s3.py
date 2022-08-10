from tempfile import NamedTemporaryFile

import boto3
import pytest
from botocore.handlers import ParamValidationError
from moto import mock_s3
from moto.s3.responses import DEFAULT_REGION_NAME

from whylogs import DatasetProfileView, ResultSet
from whylogs.api.reader.s3 import S3Reader

BUCKET_NAME = "my_bucket"
OBJECT_NAME = "my_profile.bin"


class TestS3Reader(object):
    @classmethod
    def setup_class(cls):
        mocks3 = mock_s3()
        mocks3.start()
        res = boto3.resource("s3", region_name=DEFAULT_REGION_NAME)
        res.create_bucket(Bucket=BUCKET_NAME)

    @pytest.fixture
    def object_path(self, profile_view):
        s3_client = boto3.client("s3")
        tmp_file = NamedTemporaryFile()
        with open(tmp_file.name, "rb") as f:
            profile_view.write(path=tmp_file.name)
            s3_client.upload_fileobj(f, BUCKET_NAME, OBJECT_NAME)
        return OBJECT_NAME

    def test_s3_reader(self, object_path):
        reader = S3Reader(
            bucket_name=BUCKET_NAME,
            object_name=object_path,
        )
        profile = reader.read()

        assert isinstance(profile, ResultSet)
        assert isinstance(profile.view(), DatasetProfileView)

    def test_s3_with_option(self, object_path):
        reader_option = S3Reader()
        reader_option.option(bucket_name=BUCKET_NAME, object_name=object_path)
        profile_option = reader_option.read()

        assert isinstance(profile_option, ResultSet)
        assert isinstance(profile_option.view(), DatasetProfileView)

    def test_empty_bucket_failure(self, object_path):
        reader = S3Reader(bucket_name=None, object_name=object_path)
        with pytest.raises(ParamValidationError):
            reader.read()

    def test_empty_object_name_failure(self):
        reader = S3Reader(bucket_name=BUCKET_NAME, object_name=None)
        with pytest.raises(ParamValidationError):
            reader.read()
