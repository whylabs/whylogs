import logging
import os
from typing import Any, List, Optional, Tuple, Union

from google.cloud import exceptions, storage  # type: ignore

from whylogs.api.writer import Writer
from whylogs.api.writer.writer import _Writable
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
        object_name: Optional[str] = None,  # TODO: drop this
        bucket_name: Optional[str] = None,
    ):
        self.gcs_client = gcs_client or storage.Client()
        self.object_name = object_name
        self.bucket_name = bucket_name

    @deprecated_alias(profile="file")
    def write(
        self,
        file: _Writable,
        dest: Optional[str] = None,  # TODO: this should be used as object_name
        **kwargs: Any,
    ) -> Tuple[bool, Union[str, List[Tuple[bool, str]]]]:
        success, files = file._write(**kwargs)
        files = [files] if isinstance(files, str) else files
        if not success:
            return False, "_Writable failed to create temporary file(s)"

        # TODO: support ZipFile ?

        if len(files) > 1 and ((dest or self.object_name) is not None):
            raise ValueError("Cannot specify dest or object_name for multiple files")

        bucket = self.gcs_client.bucket(self.bucket_name)
        all_success = True
        statuses = []
        for file in files:
            object_name = dest or self.object_name or file.split(os.sep)[-1]  # TODO: drop self.object_name
            blob = bucket.blob(object_name)
            try:
                blob.upload_from_filename(file)
                statuses.append((True, f"Uploaded {file} to {self.bucket_name}/{object_name}"))
                os.remove(file)
            except exceptions.Forbidden as e:
                logging.error(e)
                all_success = False
                statuses.append((False, f"{str(e)} for uploading {file} to {self.bucket_name}/{object_name}"))

        return all_success, statuses

    def option(self, **kwargs: Any) -> Writer:
        """
        bucket_name: str   GCS bucket name to write to
        object_name: str   GCS object name to create
        gcs_client: ?      GCS client object
        """
        bucket_name = kwargs.get("bucket_name")
        object_name = kwargs.get("object_name")  # TODO: drop this
        gcs_client = kwargs.get("gcs_client")
        if bucket_name:
            self.bucket_name = bucket_name
        if object_name:
            self.object_name = object_name
        if gcs_client:
            self.gcs_client = gcs_client
        return self
