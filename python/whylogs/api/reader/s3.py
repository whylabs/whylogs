from typing import Optional

import boto3

from whylogs.api.reader.reader import Reader
from whylogs.core import DatasetProfile


class S3Reader(Reader):
    def __init__(self, bucket_name: Optional[str] = None):
        self.s3_client = boto3.client("s3")
        self.bucket_name = bucket_name or ""

    def read(self, path: str, **kwargs) -> DatasetProfile:
        pass
