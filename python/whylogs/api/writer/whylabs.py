import datetime
import logging
import os
import tempfile
from typing import Any, Optional

import requests  # type: ignore
import whylabs_client
from whylabs_client.api.log_api import LogApi
from whylabs_client.model.log_async_request import LogAsyncRequest
from whylabs_client.rest import ForbiddenException

from whylogs.api.writer import Writer
from whylogs.core import DatasetProfileView
from whylogs.core.errors import BadConfigError

FIVE_MINUTES_IN_SECONDS = 60 * 5
logger = logging.getLogger(__name__)


class WhyLabsWriter(Writer):
    def __init__(self, org_id: Optional[str] = None, api_key: Optional[str] = None, dataset_id: Optional[str] = None):
        self._org_id = org_id or os.environ.get("WHYLABS_ORG_ID")
        self._api_key = api_key or os.environ.get("WHYLABS_API_KEY")
        self._dataset_id = dataset_id or os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
        self.whylabs_api_endpoint = os.environ.get("WHYLABS_API_ENDPOINT") or "https://api.whylabsapp.com"

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

    def write(self, profile: DatasetProfileView, dataset_id: Optional[str] = None) -> Any:
        if dataset_id is not None:
            self._dataset_id = dataset_id

        with tempfile.NamedTemporaryFile() as tmp_file:
            profile.write(path=tmp_file.name)
            tmp_file.flush()

            dataset_timestamp = profile.dataset_timestamp or datetime.datetime.now(datetime.timezone.utc)
            dataset_timestamp = int(dataset_timestamp.timestamp() * 1000)
            return self._upload_whylabs(dataset_timestamp=dataset_timestamp, profile_path=tmp_file.name)

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

        try:
            with open(profile_path, "rb") as f:
                http_response = requests.put(upload_url, data=f.read())
                if http_response.status_code == 200:
                    logger.info(
                        f"Done uploading {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                        f"{self.whylabs_api_endpoint} with API token ID: {self._api_key}"
                    )
                return http_response
        except requests.RequestException as e:
            logger.info(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                + f"{self.whylabs_api_endpoint}. Error occurred: {e}"
            )

    def _get_or_create_api_log_client(self) -> LogApi:
        environment_api_key = os.environ["WHYLABS_API_KEY"]
        _api_log_client = None

        if environment_api_key is not None and self._api_key != environment_api_key:
            updated_key = os.environ["WHYLABS_API_KEY"]
            logger.warning(f"Updating API key ID from: {self._api_key} to: {updated_key}")
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
        log_api = self._get_or_create_api_log_client()
        try:
            result = log_api.log_async(org_id=self._org_id, dataset_id=self._dataset_id, log_async_request=request)
            return result
        except ForbiddenException as e:
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to {self.whylabs_api_endpoint} with API token ID: {self._api_key[:10]}"
            )
            raise e

    def _get_upload_url(self, dataset_timestamp: int):
        request = self._build_log_async_request(dataset_timestamp)
        log_api = self._post_log_async(request=request, dataset_timestamp=dataset_timestamp)
        upload_url = log_api["upload_url"]
        return upload_url
