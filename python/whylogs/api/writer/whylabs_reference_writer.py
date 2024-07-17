import logging
import tempfile
from typing import Any, List, Optional, Tuple, Union
from zipfile import ZipFile

from whylabs_client import ApiClient

from whylogs.api.logger.result_set import SegmentedResultSet
from whylogs.api.whylabs.session.session_manager import INIT_DOCS
from whylogs.api.writer.whylabs_base import WhyLabsWriterBase
from whylogs.api.writer.whylabs_client import WhyLabsClient
from whylogs.api.writer.writer import _Writable
from whylogs.core import DatasetProfileView
from whylogs.core.errors import UnsupportedError
from whylogs.core.utils import deprecated_alias
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView

logger = logging.getLogger(__name__)


class WhyLabsReferenceWriter(WhyLabsWriterBase):
    f"""
    A WhyLogs writer to upload reference profiles onto the WhyLabs platform.

    >**IMPORTANT**: In order to correctly send your profiles over, make sure you have
    the following environment variables set: `[WHYLABS_ORG_ID, WHYLABS_API_KEY, WHYLABS_DEFAULT_DATASET_ID]`. You
    can also follow the authentication instructions for the why.init() method at {INIT_DOCS}.
    It is highly recommended you don't persist credentials in code!

    You shouldn't have to supply these parameters to the writer in practice. You should depend on why.init() to resolve
    the credentials for you. These are here for one-offs and testing convenience.

    Parameters
    ----------
    org_id: str, optional
        Your organization ID on the WhyLabs platform. If not informed, will get the mentioned environment
        variable instead.

    api_key: str, optional
        The API Key to authenticate with WhyLabs. If not informed, will get the mentioned environment
        variable instead.

    dataset_id: str, optional
        The Dataset ID, often referred to as model ID, that will tie the profile to your tracked data.
        If not informed, will get the mentioned environment variable instead.
    Returns
    -------
        None

    Examples
    --------
    An example usage of this method can be represented with the simple code above. Here we
    are assuming the user has already assigned a pandas.DataFrame to the `df` variable.

    ```python
    ```

    """

    def __init__(
        self,
        org_id: Optional[str] = None,
        api_key: Optional[str] = None,
        dataset_id: Optional[str] = None,
        api_client: Optional[ApiClient] = None,
        ssl_ca_cert: Optional[str] = None,
        _timeout_seconds: Optional[float] = None,
        whylabs_client: Optional[WhyLabsClient] = None,
    ):
        super().__init__(org_id, api_key, dataset_id, api_client, ssl_ca_cert, _timeout_seconds, whylabs_client)

    def _write_segmented_result_set(self, file: SegmentedResultSet, **kwargs: Any) -> Tuple[bool, str]:
        views = file.get_writables()
        if not views:
            logger.warning("Attempt to write a result set with no writables, nothing written!")
            return True, ""

        whylabs_tags = file.get_whylabs_tags()
        dataset_timestamp_epoch = self._get_dataset_epoch(views[-1])  # original writer used timestamp of last segment
        and_status: bool = True
        profile_id, upload_urls = self._whylabs_client._get_upload_urls_segmented_reference(  # type: ignore
            whylabs_tags, dataset_timestamp_epoch, self._whylabs_client._reference_profile_name
        )
        for view, url in zip(views, upload_urls):
            bool_status, message = self._upload_view(view, profile_id, url, dataset_timestamp_epoch)
            and_status = and_status and bool_status

        return and_status, (message if and_status else "Failed to upload all segments")

    # TODO: Sadly, we can't use Writer::_create_zip() because we have to fiddle with the
    # views before serializing them. We could add a Writable fiddling call-back argument
    # to _create_zip(), but this isn't too bad...
    def _write_segmented_result_set_zip(self, file: SegmentedResultSet, **kwargs: Any) -> Tuple[bool, str]:
        raise UnsupportedError("Zipped reference profiles don't work yet")
        views = file.get_writables()
        if not views:
            logger.warning("Attempt to write a result set with no writables, nothing written!")
            return True, ""

        whylabs_tags = file.get_whylabs_tags()
        dataset_timestamp_epoch = self._get_dataset_epoch(views[-1])  # original writer used timestamp of last segment
        profile_id, upload_url = self._whylabs_client._get_upload_url_segmented_reference_zip(  # type: ignore
            whylabs_tags, dataset_timestamp_epoch, self._whylabs_client._reference_profile_name
        )
        with tempfile.NamedTemporaryFile(suffix=".zip") as tmp_file:
            with ZipFile(tmp_file, "w", allowZip64=True) as zip_file:
                for view in views:
                    view = self._prepare_view_for_upload(view)
                    with tempfile.NamedTemporaryFile() as view_file:
                        view._write(file=view_file)  # TODO: error checking
                        view_file.flush()
                        view_file.seek(0)
                        zip_file.write(view_file.name, view_file.name.split("/")[-1])

            tmp_file.flush()
            tmp_file.seek(0)
            return self._whylabs_client.do_upload(  # type: ignore
                dataset_timestamp=dataset_timestamp_epoch,
                upload_url=upload_url,
                profile_id=profile_id,
                profile_file=tmp_file,
            )

    def _write_view(
        self,
        view: Union[DatasetProfileView, SegmentedDatasetProfileView],
        **kwargs: Any,
    ) -> Tuple[bool, str]:
        dataset_timestamp_epoch = self._get_dataset_epoch(view)
        profile_id, upload_url = self._whylabs_client.get_upload_url_unsegmented_reference(  # type: ignore
            dataset_timestamp_epoch, self._whylabs_client._reference_profile_name
        )
        return self._upload_view(view, profile_id, upload_url, dataset_timestamp_epoch, **kwargs)

    @deprecated_alias(profile="file")
    def write(
        self, file: _Writable, dest: Optional[str] = None, **kwargs: Any
    ) -> Tuple[bool, Union[str, List[Tuple[bool, str]]]]:
        self.option(**kwargs)
        self._custom_tagging(file)

        if isinstance(file, SegmentedResultSet):
            if kwargs.get("zip"):  # TODO: force zip if the number of segemtns is large
                return self._write_segmented_result_set_zip(file, **kwargs)
            else:
                return self._write_segmented_result_set(file, **kwargs)

        # file represents a single profile/segment, extract a view of it
        view = self._get_view_of_writable(file)
        return self._write_view(view, **kwargs)
