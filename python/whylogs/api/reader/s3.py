from tempfile import NamedTemporaryFile
from typing import Optional

import boto3

from whylogs.api.reader.reader import Reader
from whylogs.core import DatasetProfile


class S3Reader(Reader):
    def __init__(self, object_name: Optional[str] = None, bucket_name: Optional[str] = None):
        self.s3_client = boto3.client("s3")
        self.object_name = object_name or None
        self.bucket_name = bucket_name or ""

    def read(self, **kwargs) -> DatasetProfile:
        with NamedTemporaryFile() as tmp_file:
            self.s3_client.download_file(Bucket=self.bucket_name, Key=self.object_name, Filename=tmp_file.name)
            tmp_file.flush()
            result_set = self.get_file_from_path(path=tmp_file.name)
        return result_set

    def option(self, object_name: Optional[str] = None, bucket_name: Optional[str] = None) -> None:
        if object_name is not None:
            self.object_name = object_name
        if bucket_name is not None:
            self.bucket_name = bucket_name
