import errno
import logging
import os
import random
import socket

import pytest
from gcp_storage_emulator.server import create_server
from google.cloud import storage

from whylogs.api.writer.gcs import GCSWriter

HOST = "localhost"
PORT = 9023
MAX_PORT = 10000
GCS_BUCKET = "test-bucket"
TEST_LOGGER = logging.getLogger(__name__)


class TestGCSWriter(object):
    server = None
    test_port = PORT

    @classmethod
    def _find_open_test_port(cls, max_retry: int):
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        retry_count = 0
        while retry_count < max_retry:
            try:
                test_socket.bind(("127.0.0.1", cls.test_port))
                return True
            except socket.error as e:
                if e.errno == errno.EADDRINUSE:
                    port = random.randint(PORT + 1, MAX_PORT)
                    cls.test_port = port
                    TEST_LOGGER.warning(f"attempt #{retry_count + 1} will try port: {cls.test_port}")

                else:
                    TEST_LOGGER.info(
                        f"{e} wasn't address in use error when attempting to bind to port so letting test proceed."
                    )
                    return True
            finally:
                retry_count = retry_count + 1
                test_socket.close()
        return False

    @classmethod
    def setup_class(cls):
        assert TestGCSWriter._find_open_test_port(max_retry=3)
        storage_host = f"http://{HOST}:{cls.test_port}"
        TEST_LOGGER.info(f"Running test using {storage_host}")
        os.environ["STORAGE_EMULATOR_HOST"] = storage_host
        cls.server = create_server(HOST, cls.test_port, in_memory=True, default_bucket=GCS_BUCKET)
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
