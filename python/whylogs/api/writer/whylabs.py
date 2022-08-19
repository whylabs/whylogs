import datetime
import logging
import os
import tempfile
from typing import Any, Optional, Tuple

import requests  # type: ignore
import whylabs_client
from whylabs_client.api.log_api import LogApi
from whylabs_client.model.log_async_request import LogAsyncRequest
from whylabs_client.model.log_reference_request import LogReferenceRequest
from whylabs_client.rest import ForbiddenException

from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.core import DatasetProfileView
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.errors import BadConfigError
from whylogs.core.utils import deprecated_alias

FIVE_MINUTES_IN_SECONDS = 60 * 5
logger = logging.getLogger(__name__)


class WhyLabsWriter(Writer):
    """
    A WhyLogs writer to upload DatasetProfileView's onto the WhyLabs platform.

    >**IMPORTANT**: In order to correctly send your profiles over, make sure you have
    the following environment variables set: `[WHYLABS_ORG_ID, WHYLABS_API_KEY, WHYLABS_DEFAULT_DATASET_ID]`. You
    can also set them with the option method or within the constructor, although it
    is highly recommended you don't persist credentials in code!

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
    import whylogs as why

    profile = why.log(pandas=df)
    profile.writer("whylabs").write()
    ```

    """

    def __init__(self, org_id: Optional[str] = None, api_key: Optional[str] = None, dataset_id: Optional[str] = None):
        self._org_id = org_id or os.environ.get("WHYLABS_DEFAULT_ORG_ID")
        self._api_key = api_key or os.environ.get("WHYLABS_API_KEY")
        self._dataset_id = dataset_id or os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
        self.whylabs_api_endpoint = os.environ.get("WHYLABS_API_ENDPOINT") or "https://api.whylabsapp.com"
        self._reference_profile_name = os.environ.get("WHYLABS_REFERENCE_PROFILE_NAME")

    def check_interval(self, interval_seconds: int):
        if interval_seconds < FIVE_MINUTES_IN_SECONDS:
            raise BadConfigError("Bad WhyLabsWriter config: interval must be greater or equal to five minutes")

    def option(
        self,
        org_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        api_key: Optional[str] = None,
        reference_profile_name: Optional[str] = None,
    ) -> "WhyLabsWriter":
        if dataset_id is not None:
            self._dataset_id = dataset_id
        if org_id is not None:
            self._org_id = org_id
        if api_key is not None:
            self._api_key = api_key
        if reference_profile_name is not None:
            self._reference_profile_name = reference_profile_name
        return self

    @deprecated_alias(profile="file")
    def write(self, file: Writable, **kwargs: Any) -> None:
        profile_view = file.view() if isinstance(file, DatasetProfile) else file

        if not isinstance(profile_view, DatasetProfileView):
            raise ValueError(
                "You must pass either a DatasetProfile or a DatasetProfileView in order to use this writer!"
            )

        if kwargs.get("dataset_id") is not None:
            self._dataset_id = kwargs.get("dataset_id")

        with tempfile.NamedTemporaryFile() as tmp_file:
            profile_view.write(path=tmp_file.name)
            tmp_file.flush()

            dataset_timestamp = profile_view.dataset_timestamp or datetime.datetime.now(datetime.timezone.utc)
            dataset_timestamp = int(dataset_timestamp.timestamp() * 1000)
            self._upload_whylabs(
                dataset_timestamp=dataset_timestamp,
                profile_path=tmp_file.name,
                reference_profile_name=self._reference_profile_name,
            )

    @staticmethod
    def _check_api_key_format(input_key: str) -> Tuple[bool, Optional[str]]:
        if input_key is None or len(input_key) < 12 or input_key[10] != "." or len(input_key.split(".")) != 2:
            message = None
            if input_key is None:
                message = "api_key is None"
            elif len(input_key) < 12:
                message = "api_key length < 12"
            elif input_key[10] != ".":
                message = "api_key must have a period delimiter at index 10"
            else:
                delimiter_count = len(input_key.split(".")) - 1
                message = (
                    f"api_key must have a single period delimiter but {delimiter_count} delimiters found in string"
                )
            return (False, message)
        return (True, None)

    def _upload_whylabs(
        self,
        dataset_timestamp: int,
        profile_path: str,
        upload_url: Optional[str] = None,
        reference_profile_name: Optional[str] = None,
    ) -> requests.Response:
        if self._org_id is None:
            raise EnvironmentError(
                "Missing organization ID. Specify it via option or WHYLABS_DEFAULT_ORG_ID " "environment variable"
            )
        if self._dataset_id is None:
            raise EnvironmentError(
                "Missing dataset ID. Specify it via WHYLABS_DEFAULT_DATASET_ID environment "
                "variable or on your write method"
            )

        upload_url = upload_url or self._get_upload_url(
            dataset_timestamp=dataset_timestamp, reference_profile_name=reference_profile_name
        )
        api_key_id = self._api_key[:10] if self._api_key else None
        try:
            with open(profile_path, "rb") as f:
                http_response = requests.put(upload_url, data=f.read())
                if http_response.status_code == 200:
                    logger.info(
                        f"Done uploading {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                        f"{self.whylabs_api_endpoint} with API token ID: {api_key_id}"
                    )
                return http_response
        except requests.RequestException as e:
            logger.info(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                + f"{self.whylabs_api_endpoint} with API token ID: {api_key_id}. Error occurred: {e}"
            )

    def _get_or_create_api_log_client(self) -> LogApi:
        environment_api_key = os.environ.get("WHYLABS_API_KEY")
        _api_log_client = None

        if environment_api_key is not None and self._api_key != environment_api_key:
            updated_key = os.environ.get("WHYLABS_API_KEY")
            old_id = self._api_key[:10] if self._api_key else None
            new_id = updated_key[:10] if updated_key else None
            logger.warning(f"Updating API key ID from: {old_id} to: {new_id}")
            self._api_key = updated_key
            config = whylabs_client.Configuration(
                host=self.whylabs_api_endpoint, api_key={"ApiKeyAuth": self._api_key}, discard_unknown_keys=True
            )
            _api_log_client = whylabs_client.ApiClient(config)

        if _api_log_client is None:
            config = whylabs_client.Configuration(
                host=self.whylabs_api_endpoint, api_key={"ApiKeyAuth": self._api_key}, discard_unknown_keys=True
            )
            _api_log_client = whylabs_client.ApiClient(config)
        return LogApi(_api_log_client)

    @staticmethod
    def _build_log_async_request(dataset_timestamp):
        request = LogAsyncRequest(dataset_timestamp=dataset_timestamp, segment_tags=[])
        return request

    @staticmethod
    def _build_log_reference_request(dataset_timestamp, alias: Optional[str] = None):
        request = LogReferenceRequest(dataset_timestamp=dataset_timestamp, alias=alias)
        return request

    def _post_log_async(self, request, dataset_timestamp):
        api_key_valid, validation_message = self._check_api_key_format(input_key=self._api_key)
        if not api_key_valid:
            api_key_id = self._api_key[:10] if self._api_key and len(self._api_key) > 11 else None
            raise ValueError(
                f"WhyLabs API Key invalid! Because: {validation_message}. ID portion was: [{api_key_id}]."
                f" Upload failed for {self._org_id}/{self._dataset_id}/{dataset_timestamp}"
            )

        log_api = self._get_or_create_api_log_client()
        try:
            result = log_api.log_async(org_id=self._org_id, dataset_id=self._dataset_id, log_async_request=request)
            return result
        except ForbiddenException as e:
            api_key_id = self._api_key[:10] if self._api_key else None
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to {self.whylabs_api_endpoint}"
                f" with API token ID: {api_key_id}"
            )
            raise e

    def _post_log_reference(self, request, dataset_timestamp):
        api_key_valid, validation_message = self._check_api_key_format(input_key=self._api_key)
        if not api_key_valid:
            api_key_id = self._api_key[:10] if self._api_key and len(self._api_key) > 11 else None
            raise ValueError(
                f"WhyLabs API Key invalid! Because: {validation_message}. ID portion was: [{api_key_id}]."
                f" Upload failed for {self._org_id}/{self._dataset_id}/{dataset_timestamp}"
            )

        log_api = self._get_or_create_api_log_client()
        try:
            async_result = log_api.log_reference(
                org_id=self._org_id, model_id=self._dataset_id, log_reference_request=request, async_req=True
            )
            result = async_result.get()
            return result
        except ForbiddenException as e:
            api_key_id = self._api_key[:10] if self._api_key else None
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to {self.whylabs_api_endpoint}"
                f" with API token ID: {api_key_id}"
            )
            raise e

    def _get_upload_url(self, dataset_timestamp: int, reference_profile_name: Optional[str] = None):
        if reference_profile_name is None:
            request = self._build_log_async_request(dataset_timestamp)
            log_api = self._post_log_async(request=request, dataset_timestamp=dataset_timestamp)
            upload_url = log_api["upload_url"]
        else:
            request = self._build_log_reference_request(dataset_timestamp, alias=reference_profile_name)
            log_api = self._post_log_reference(request=request, dataset_timestamp=dataset_timestamp)
            upload_url = log_api["upload_url"]
        return upload_url
