import datetime
import logging
import os
import tempfile
from typing import Any, Optional, Tuple, Union

import requests  # type: ignore
import whylabs_client
from whylabs_client.api.log_api import LogApi
from whylabs_client.model.log_async_request import LogAsyncRequest
from whylabs_client.rest import ForbiddenException

from whylogs.api.writer import Writer
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
    the following environment variables set: `[WHYLABS_ORG_ID, WHYLABS_API_KEY]`. You
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
        self._whylabs_v1_enabled = os.environ.get("WHYLABS_V1_ENABLED")

    def check_interval(self, interval_seconds: int):
        if interval_seconds < FIVE_MINUTES_IN_SECONDS:
            raise BadConfigError("Bad WhyLabsWriter config: interval must be greater or equal to five minutes")

    def option(
        self, org_id: Optional[str] = None, dataset_id: Optional[str] = None, api_key: Optional[str] = None
    ) -> None:
        if dataset_id is not None:
            self._dataset_id = dataset_id
        if org_id is not None:
            self._org_id = org_id
        if api_key is not None:
            self._api_key = api_key

    @deprecated_alias(profile="file")
    def write(self, file: Union[DatasetProfileView, DatasetProfile], dataset_id: Optional[str] = None, **kwargs) -> Any:
        # check if the server supports ingesting whylogs 1.0.x profiles:
        if self._check_if_whylabs_disabled_v1_profiles():
            raise ValueError("The Whylabs writer is not yet supported on whylogs 1.0.x!")

        profile_view = file.view() if isinstance(file, DatasetProfile) else file

        if not isinstance(profile_view, DatasetProfileView):
            raise BadConfigError(
                "You must pass either a DatasetProfile or a DatasetProfileView" "in order to use this writer!"
            )

        if dataset_id is not None:
            self._dataset_id = dataset_id

        with tempfile.NamedTemporaryFile() as tmp_file:
            profile_view.write(path=tmp_file.name)
            tmp_file.flush()

            dataset_timestamp = profile_view.dataset_timestamp or datetime.datetime.now(datetime.timezone.utc)
            dataset_timestamp = int(dataset_timestamp.timestamp() * 1000)
            return self._upload_whylabs(dataset_timestamp=dataset_timestamp, profile_path=tmp_file.name)

    # TODO: remove once this is supported, decoupling support from release for now
    def _check_if_whylabs_disabled_v1_profiles(self) -> bool:
        whylabs_config_url = "https://whylabs-public.s3.us-west-2.amazonaws.com/whylogs_config/whylabs_writer_disabled"
        logger.info(f"checking: {whylabs_config_url}")
        response = requests.head(whylabs_config_url)
        logger.info(f"checking: {whylabs_config_url}")
        logger.info(f"headers are: {response.headers} code: {response.status_code}")
        if response.status_code == 200:
            logger.info(f"found the disabled config, falling back to env var: {self._whylabs_v1_enabled}")
            return not self._whylabs_v1_enabled
        logger.info("no whylabs disabled config found, so allowing upload to whylabs!")
        return False

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
        self, dataset_timestamp: int, profile_path: str, upload_url: Optional[str] = None
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

        upload_url = upload_url or self._get_upload_url(dataset_timestamp=dataset_timestamp)
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

    def _get_upload_url(self, dataset_timestamp: int):
        request = self._build_log_async_request(dataset_timestamp)
        log_api = self._post_log_async(request=request, dataset_timestamp=dataset_timestamp)
        upload_url = log_api["upload_url"]
        return upload_url
