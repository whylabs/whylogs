from tempfile import NamedTemporaryFile
from typing import Optional

import boto3

from whylogs.api.reader.reader import Reader
from whylogs.core import DatasetProfile


class S3Reader(Reader):
    def __init__(self, object_name: str, bucket_name: Optional[str] = None):
        self.s3_client = boto3.client("s3")
        self.object_name = object_name
        self.bucket_name = bucket_name or ""

    def read(self, **kwargs) -> DatasetProfile:
        tmp_file = NamedTemporaryFile()

        with open(tmp_file.name, "wb") as f:
            self.s3_client.download_fileobj(Bucket=self.bucket_name, Key=self.object_name, Fileobj=f)
            result_set = self.get_file_from_path(path=tmp_file.name)
        return result_set

    @classmethod
    def option(cls, object_name: Optional[str] = None, bucket_name: Optional[str] = None):
        if object_name is not None:
            cls.object_name = object_name

        if bucket_name is not None:
            cls.bucket_name = bucket_name
