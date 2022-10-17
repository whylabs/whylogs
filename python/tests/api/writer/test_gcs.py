import os

import pytest
from gcp_storage_emulator.server import create_server
from google.cloud import storage

from whylogs.api.writer.gcs import GCSWriter

HOST = "localhost"
PORT = 9023
GCS_BUCKET = "test-bucket"


class TestGCSWriter(object):
    server = None

    @classmethod
    def setup_class(cls):
        os.environ["STORAGE_EMULATOR_HOST"] = f"http://{HOST}:{PORT}"
        cls.server = create_server(HOST, PORT, in_memory=True, default_bucket=GCS_BUCKET)
        cls.server.start()

    @classmethod
    def teardown_class(cls):
        del os.environ["STORAGE_EMULATOR_HOST"]
        cls.server.stop()

    @pytest.fixture
    def bucket(self):
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        return bucket

    def test_gcs_writer(self, bucket, profile_view):
        writer = GCSWriter()
        result = writer.option(bucket_name=GCS_BUCKET, object_name="my_object.bin").write(file=profile_view)

        assert result[0] is True
        assert f"{GCS_BUCKET}/my_object.bin" in result[1]

        blob_names = []
        for blob in bucket.list_blobs():
            blob_names.append(blob.name)
        assert "my_object.bin" in blob_names

    def test_result_set_writer(self, bucket, result_set, tmp_path):
        result_set.writer("gcs").option(bucket_name=GCS_BUCKET).write(dest=tmp_path.name)
        blob_names = []
        for blob in bucket.list_blobs():
            blob_names.append(blob.name)
        assert tmp_path.name in blob_names
