from tempfile import NamedTemporaryFile
from typing import Optional

from botocore.client import BaseClient
from botocore.config import Config

from whylogs import ResultSet
from whylogs.api._s3_client import build_s3_client
from whylogs.api.reader.reader import Reader


class S3Reader(Reader):
    """
    A WhyLogs reader to fetch written profiles from an Amazon S3 bucket.

    >**IMPORTANT**: In order to correctly connect to your Amazon S3 bucket, make sure you have
    the following environment variables set: `[AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]`.

    The same reader targets any S3-compatible object store by passing an ``endpoint_url``
    (and matching credentials). To reuse a fully configured client instead, pass it as
    ``s3_client`` and it is used as provided.

    Parameters
    ----------
    bucket_name: str, optional
        The name of the bucket to connect to. Made optional so the user can also access it
        via the `option` method
    object_name: str, optional
        The s3's object name. It basically states the location where the file goes to.
        Also made optional, so it can be defined through the `option` method
    s3_client: BaseClient, optional
        A boto3 S3 client. When omitted, a client is built internally.
    endpoint_url: str, optional
        Endpoint URL of an S3-compatible object store. Leave unset for AWS S3.
        Only applies to the internally built client.
    config: botocore.config.Config, optional
        Extra botocore configuration merged into the internally built client.

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
        endpoint_url: Optional[str] = None,
        config: Optional[Config] = None,
    ):
        self.s3_client = s3_client or build_s3_client(config=config, endpoint_url=endpoint_url)
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
