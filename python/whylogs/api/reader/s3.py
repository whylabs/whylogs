from tempfile import NamedTemporaryFile
from typing import Optional

import boto3

from whylogs import ResultSet
from whylogs.api.reader.reader import Reader


class S3Reader(Reader):
    """
    A WhyLogs reader to fetch written profiles from an Amazon S3 bucket.

    >**IMPORTANT**: In order to correctly connect to your Amazon S3 bucket, make sure you have
    the following environment variables set: `[AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]`.

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

    def __init__(self, object_name: Optional[str] = None, bucket_name: Optional[str] = None):
        self.s3_client = boto3.client("s3")
        self.object_name = object_name or None
        self.bucket_name = bucket_name or ""

    def read(self, **kwargs) -> ResultSet:
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
