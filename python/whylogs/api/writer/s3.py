import logging
import tempfile
from typing import Any, Optional, Tuple

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.core.utils import deprecated_alias

logger = logging.getLogger(__name__)


class S3Writer(Writer):
    """
    A WhyLogs writer to upload DatasetProfileViews onto Amazon S3.

    >**IMPORTANT**: In order to correctly connect to your Amazon S3 bucket, make sure you have
    the following environment variables set: `[AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]`

    Parameters
    ----------
    s3_client: BaseClient, optional
        The s3 client used to authenticate and perform operations on the s3 bucket.
        Should be a BaseClient from the boto3 library
    base_prefix: str, optional
        The base file prefix for s3, in order to organize. A placeholder 'profile' will take place if None is provided.
    bucket_name: str, optional
        The name of the bucket to connect to. Made optional so the user can also access it
        via the `option` method
    object_name: str, optional
        The s3's object name. It basically states the location where the file goes to.
        Also made optional, so it can be defined through the `option` method
    Returns
    -------
        None

    Examples
    --------
    An example usage of this method can be represented with the simple code above. Here we
    are assuming the user has already assigned a pandas.DataFrame to the `df` variable.

    ```python
    import whylogs as why

    profile = why.log(pandas=df)
    profile.writer("s3").option(bucket_name="my_bucket").write()
    ```

    """

    def __init__(
        self,
        s3_client: Optional[BaseClient] = None,
        base_prefix: Optional[str] = None,
        bucket_name: Optional[str] = None,
        object_name: Optional[str] = None,
    ):
        self.s3_client = s3_client or boto3.client("s3")
        self.base_prefix = base_prefix or "profile"
        self.bucket_name = bucket_name or ""
        self.object_name = object_name or None

    @deprecated_alias(profile="file")
    def write(
        self,
        file: Writable,
        dest: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bool, str]:
        dest = dest or file.get_default_path()  # type: ignore
        if self.object_name is None:
            self.object_name = dest
        try:
            with tempfile.NamedTemporaryFile() as tmp_file:
                file.write(path=tmp_file.name)  # type: ignore
                tmp_file.flush()
                self.s3_client.upload_file(tmp_file.name, self.bucket_name, self.object_name)
        except ClientError as e:
            logging.error(e)
            return False, str(e)
        return True, f"Uploaded {tmp_file.name} to {self.bucket_name}/{self.object_name}"

    def option(
        self,
        bucket_name: Optional[str] = None,
        object_name: Optional[str] = None,
        s3_client: Optional[BaseClient] = None,
    ) -> "S3Writer":  # type: ignore
        if bucket_name:
            self.bucket_name = bucket_name
        if object_name:
            self.object_name = object_name
        if s3_client:
            self.s3_client = s3_client
        return self
