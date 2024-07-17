import datetime
import logging
from typing import Any, List, Optional, Tuple, Union

from whylabs_client import ApiClient

from whylogs.api.logger.result_set import SegmentedResultSet
from whylogs.api.whylabs.session.session_manager import INIT_DOCS
from whylogs.api.writer.whylabs_base import WhyLabsWriterBase
from whylogs.api.writer.whylabs_client import WhyLabsClient
from whylogs.api.writer.writer import _Writable
from whylogs.core import DatasetProfileView
from whylogs.core.utils import deprecated_alias
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView

logger = logging.getLogger(__name__)


class WhyLabsTransactionWriter(WhyLabsWriterBase):
    f"""
    A WhyLogs writer to upload profiles onto the WhyLabs platform as a transaction.

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
        transaction_id: Optional[str] = None,
    ):
        super().__init__(org_id, api_key, dataset_id, api_client, ssl_ca_cert, _timeout_seconds, whylabs_client)
        transaction_id = transaction_id or self._whylabs_client.get_transaction_id()  # type: ignore
        self._whylabs_client._transaction_id = transaction_id  # type: ignore
        self._aborted: bool = False

    @property
    def transaction_id(self) -> Optional[str]:
        return self._whylabs_client._transaction_id  # type: ignore

    def _write_segmented_result_set(self, file: SegmentedResultSet, **kwargs: Any) -> Tuple[bool, str]:
        views = file.get_writables()
        if not views:
            logger.warning("Attempt to write a result set with no writables, nothing written!")
            return True, ""

        whylabs_tags = file.get_whylabs_tags()
        utc_now = datetime.datetime.now(datetime.timezone.utc)
        and_status: bool = True
        logger.debug(f"About to write {len(views)} files:")
        for view, tags in zip(views, whylabs_tags):
            dataset_timestamp_epoch = self._get_dataset_epoch(view, utc_now)
            profile_id, upload_url = self._whylabs_client.get_upload_url_transaction(dataset_timestamp_epoch, tags)  # type: ignore
            bool_status, message = self._upload_view(
                view, profile_id, upload_url, dataset_timestamp_epoch, tags, **kwargs
            )
            if not bool_status:
                self._whylabs_client.abort_transaction(self._whylabs_client._transaction_id)  # type: ignore
                self._aborted = True
                logger.info(
                    f"Uploading {profile_id} failed; aborting transaction {self._whylabs_client._transaction_id}"  # type: ignore
                )
            else:
                logger.info(f"Added profile {profile_id} to transaction {self._whylabs_client._transaction_id}")  # type: ignore
            and_status = and_status and bool_status

        logger.debug(f"Completed writing {len(views)} files!")
        return and_status, profile_id if and_status else "Failed to upload all segments"

    def _write_view(
        self,
        view: Union[DatasetProfileView, SegmentedDatasetProfileView],
        **kwargs: Any,
    ) -> Tuple[bool, str]:
        dataset_timestamp_epoch = self._get_dataset_epoch(view)
        profile_id, upload_url = self._whylabs_client.get_upload_url_transaction(dataset_timestamp_epoch)  # type: ignore
        logger.info(f"Added profile {profile_id} to transaction {self._whylabs_client._transaction_id}")  # type: ignore
        success, message = self._upload_view(view, profile_id, upload_url, dataset_timestamp_epoch, **kwargs)
        if not success:
            self._whylabs_client.abort_transaction(self.transaction_id)  # type: ignore
            self._aborted = True
            logger.info(f"Uploading {profile_id} failed; aborting transaction {self._whylabs_client._transaction_id}")  # type: ignore
        return success, message

    @deprecated_alias(profile="file")
    def write(
        self, file: _Writable, dest: Optional[str] = None, **kwargs: Any
    ) -> Tuple[bool, Union[str, List[Tuple[bool, str]]]]:
        if self._aborted:
            return False, f"Transaction {self._whylabs_client._transaction_id} was aborted"  # type: ignore

        transaction_id = kwargs.get("transaction_id") or self._whylabs_client._transaction_id  # type: ignore
        if not transaction_id:
            raise ValueError("Missing transaction id")

        self._whylabs_client = self._whylabs_client.option(**kwargs)  # type: ignore
        self._custom_tagging(file)
        if isinstance(file, SegmentedResultSet):
            return self._write_segmented_result_set(file, **kwargs)

        # file represents a single profile/segment, extract a view of it
        view = self._get_view_of_writable(file)
        return self._write_view(view, **kwargs)

    def __enter__(self) -> "WhyLabsTransactionWriter":
        if self._whylabs_client._transaction_id is None:
            self._whylabs_client._transaction_id = self._whylabs_client.get_transaction_id()

        return self

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        id = self._whylabs_client._transaction_id
        self._whylabs_client._transaction_id = None  # type: ignore
        self._whylabs_client.commit_transaction(id)  # type: ignore
