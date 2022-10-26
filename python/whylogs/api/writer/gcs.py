import logging
import tempfile
from typing import Any, Optional, Tuple

from google.cloud import exceptions, storage  # type: ignore

from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.core.utils import deprecated_alias

logger = logging.getLogger(__name__)


class GCSWriter(Writer):
    """
    A whylogs writer to upload DatasetProfileViews onto Google Cloud Storage (GCS).

    >**IMPORTANT**: In order to correctly connect to your GCS container, make sure you have
    the following environment variables set: `[GOOGLE_APPLICATION_CREDENTIALS]`

    Parameters
    ----------
    bucket_name = "your-bucket-name"
    source_file_name = "local/path/to/file"
    destination_blob_name = "storage-object-name"

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
    profile.writer("gcs").option(bucket_name="my_bucket").write()
    ```
    """

    def __init__(
        self,
        gcs_client: Optional[storage.Client] = None,
        object_name: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ):
        self.gcs_client = gcs_client or storage.Client()
        self.object_name = object_name
        self.bucket_name = bucket_name

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
        bucket = self.gcs_client.bucket(self.bucket_name)
        blob = bucket.blob(self.object_name)
        try:
            with tempfile.NamedTemporaryFile() as tmp_file:
                file.write(path=tmp_file.name)  # type: ignore
                tmp_file.flush()
                blob.upload_from_filename(tmp_file.name)
        except exceptions.Forbidden as e:
            logging.error(e)
            return False, str(e)
        return True, f"Uploaded {tmp_file.name} to {self.bucket_name}/{self.object_name}"

    def option(
        self,
        bucket_name: Optional[str] = None,
        object_name: Optional[str] = None,
        gcs_client: Optional[Any] = None,
    ) -> "GCSWriter":  # type: ignore
        if bucket_name:
            self.bucket_name = bucket_name
        if object_name:
            self.object_name = object_name
        if gcs_client:
            self.gcs_client = gcs_client
        return self
