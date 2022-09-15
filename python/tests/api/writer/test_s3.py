import boto3
import pytest
from botocore.handlers import ParamValidationError
from moto import mock_s3
from moto.s3.responses import DEFAULT_REGION_NAME

from whylogs.api.writer.s3 import S3Writer

BUCKET_NAME = "my_bucket"


class TestS3Writer(object):
    @classmethod
    def setup_class(cls):
        mocks3 = mock_s3()
        mocks3.start()
        res = boto3.resource("s3", region_name=DEFAULT_REGION_NAME)
        res.create_bucket(Bucket=BUCKET_NAME)

    def test_s3_writer(self, tmp_path, profile_view):
        writer = S3Writer()
        writer.option(bucket_name=BUCKET_NAME, object_name=tmp_path.name)
        writer.write(profile=profile_view, dest=tmp_path.name)

        objects = writer.s3_client.list_objects(Bucket=BUCKET_NAME)
        assert tmp_path.name in [obj["Key"] for obj in objects.get("Contents", [])]
        
    def test_s3_writer_response(self, tmp_path, profile_view):
        writer = S3Writer()
        writer.option(bucket_name=BUCKET_NAME, object_name=tmp_path.name)
        response = writer.write(profile=profile_view, dest=tmp_path.name)
        assert response == 1

    def test_s3_writer_with_api_call(self, result_set, tmp_path):
        result_set.writer("s3").option(bucket_name=BUCKET_NAME).write(dest=tmp_path.name)

        writer = S3Writer()
        objects = writer.s3_client.list_objects(Bucket=BUCKET_NAME)
        assert tmp_path.name in [obj["Key"] for obj in objects.get("Contents", [])]

    def test_s3_writer_with_html_report(self, html_report, tmp_path):
        html_report.writer("s3").option(bucket_name=BUCKET_NAME).write(dest=tmp_path.name)

        writer = S3Writer()
        objects = writer.s3_client.list_objects(Bucket=BUCKET_NAME)
        assert tmp_path.name in [obj["Key"] for obj in objects.get("Contents", [])]

    def test_empty_string_bucket_name_raises_exception(self, result_set):
        with pytest.raises(ParamValidationError):
            result_set.writer("s3").option(bucket_name="").write()

    def test_empty_string_object_name_writes_to_default_path(self, result_set):
        result_set.writer("s3").option(object_name="", bucket_name=BUCKET_NAME).write(dest=None)

        writer = S3Writer()
        objects = writer.s3_client.list_objects(Bucket=BUCKET_NAME)
        expected_path = result_set.view().get_default_path()

        assert expected_path in [obj["Key"] for obj in objects.get("Contents", [])]
