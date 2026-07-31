from tempfile import NamedTemporaryFile
from typing import Optional

import boto3
from botocore.client import BaseClient
from botocore.config import Config

from whylogs import ResultSet, __version__
from whylogs.api.reader.reader import Reader

_USER_AGENT_EXTRA = f"whylogs/python/{__version__}"


class S3Reader(Reader):
    """
    A WhyLogs reader to fetch written profiles from an Amazon S3 bucket.

    >**IMPORTANT**: In order to correctly connect to your Amazon S3 bucket, make sure you have
    the following environment variables set: `[AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]`.

    To read from an S3-compatible object store instead (for example Backblaze B2, Cloudflare R2,
    or MinIO), pass a client built with that store's endpoint, such as
    `S3Reader(s3_client=boto3.client("s3", endpoint_url="https://s3.example-region.example.com"))`.

    Parameters
    ----------
    bucket_name: str, optional
        The name of the bucket to connect to. Made optional so the user can also access it
        via the `option` method
    object_name: str, optional
        The s3's object name. It basically states the location where the file goes to.
        Also made optional, so it can be defined through the `option` method

    Examples
    --------
    An example usage of this method can be represented with the simple code below.

    ```python
    import whylogs as why

    profile = why.reader("s3").option(bucket_name="my_bucket", object_name="my/object.bin").read()
    ```
    """

    def __init__(
        self,
        object_name: Optional[str] = None,
        bucket_name: Optional[str] = None,
        s3_client: Optional[BaseClient] = None,
    ):
        self.s3_client = s3_client or boto3.client("s3", config=Config(user_agent_extra=_USER_AGENT_EXTRA))
        self.object_name = object_name or None
        self.bucket_name = bucket_name or ""

    def read(self, **kwargs) -> ResultSet:
        with NamedTemporaryFile() as tmp_file:
            self.s3_client.download_file(Bucket=self.bucket_name, Key=self.object_name, Filename=tmp_file.name)
            tmp_file.flush()
            result_set = self.get_file_from_path(path=tmp_file.name)
        return result_set

    def option(self, object_name: Optional[str] = None, bucket_name: Optional[str] = None) -> "S3Reader":
        if object_name is not None:
            self.object_name = object_name
        if bucket_name is not None:
            self.bucket_name = bucket_name
        return self
