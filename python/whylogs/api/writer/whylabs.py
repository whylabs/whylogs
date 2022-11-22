import datetime
import logging
import os
import tempfile
from typing import IO, Any, Optional, Tuple

import requests  # type: ignore
import whylabs_client  # type: ignore
from whylabs_client import ApiClient, Configuration  # type: ignore
from whylabs_client.api.feature_weights_api import FeatureWeightsApi
from whylabs_client.api.log_api import AsyncLogResponse  # type: ignore
from whylabs_client.api.log_api import (
    LogApi,
    LogAsyncRequest,
    LogReferenceRequest,
    LogReferenceResponse,
)
from whylabs_client.rest import ForbiddenException  # type: ignore

from whylogs import __version__ as _version
from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.core import DatasetProfileView
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.errors import BadConfigError
from whylogs.core.feature_weights import FeatureWeights
from whylogs.core.utils import deprecated_alias
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView
from whylogs.migration.uncompound import (
    _uncompound_dataset_profile,
    _uncompound_metric_feature_flag,
)

FIVE_MINUTES_IN_SECONDS = 60 * 5
DAY_IN_SECONDS = 60 * 60 * 24
WEEK_IN_SECONDS = DAY_IN_SECONDS * 7
FIVE_YEARS_IN_SECONDS = DAY_IN_SECONDS * 365 * 5
logger = logging.getLogger(__name__)

API_KEY_ENV = "WHYLABS_API_KEY"


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
        self._api_key_id = ""
        self._dataset_id = dataset_id or os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
        self._feature_weights = None
        self.whylabs_api_endpoint = os.environ.get("WHYLABS_API_ENDPOINT") or "https://api.whylabsapp.com"
        self._reference_profile_name = os.environ.get("WHYLABS_REFERENCE_PROFILE_NAME")
        self._api_log_client: Optional[ApiClient] = None
        self._ssl_ca_cert: Optional[str] = None
        self._configuration: Optional[Configuration] = None

    def check_interval(self, interval_seconds: int) -> None:
        if interval_seconds < FIVE_MINUTES_IN_SECONDS:
            raise BadConfigError("Bad WhyLabsWriter config: interval must be greater or equal to five minutes")

    def option(  # type: ignore
        self,
        org_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        api_key: Optional[str] = None,
        reference_profile_name: Optional[str] = None,
        configuration: Optional[Configuration] = None,
        ssl_ca_cert: Optional[str] = None,
    ) -> "WhyLabsWriter":
        """

        Parameters
        ----------
        org_id the organization ID
        dataset_id the dataset Id
        api_key the API key
        reference_profile_name the name of the reference profile
        configuration the additional configuration for the REST client

        Returns a "WhyLabsWriter" with these options configured
        -------

        """
        if dataset_id is not None:
            self._dataset_id = dataset_id
        if org_id is not None:
            self._org_id = org_id
        if api_key is not None:
            self._api_key = api_key
        if reference_profile_name is not None:
            self._reference_profile_name = reference_profile_name
        if configuration is not None:
            self._configuration = configuration
        if ssl_ca_cert is not None:
            self._ssl_ca_cert = ssl_ca_cert
        return self

    def write_feature_weights(self, file: FeatureWeights, **kwargs: Any) -> Tuple[bool, str]:
        """Put feature weights for the specified dataset.

        Parameters
        ----------
        file : FeatureWeights
            FeatureWeights object representing the Feature Weights for the specified dataset

        Returns
        -------
        Tuple[bool, str]
            Tuple with a boolean (1-success, 0-fail) and string with the request's status code.
        """
        self._feature_weights = file.to_dict()
        if kwargs.get("dataset_id") is not None:
            self._dataset_id = kwargs.get("dataset_id")
        return self._do_upload_feature_weights()

    def get_feature_weights(self, **kwargs: Any) -> Optional[FeatureWeights]:
        """Get latest version for the feature weights for the specified dataset

        Returns
        -------
        FeatureWeightResponse
            Response of the GET request, with segmentWeights and metadata.
        """
        if kwargs.get("dataset_id") is not None:
            self._dataset_id = kwargs.get("dataset_id")

        result = self._do_get_feature_weights()
        feature_weights_set = result.get("segmentWeights")
        metadata = result.get("metadata")
        if feature_weights_set and isinstance(feature_weights_set, list):
            feature_weights = FeatureWeights(weights=feature_weights_set[0]["weights"], metadata=metadata)
            return feature_weights
        return None

    @deprecated_alias(profile="file")
    def write(self, file: Writable, **kwargs: Any) -> Tuple[bool, str]:

        if isinstance(file, FeatureWeights):
            return self.write_feature_weights(file, **kwargs)

        view = file.view() if isinstance(file, DatasetProfile) else file
        has_segments = isinstance(view, SegmentedDatasetProfileView)

        if not has_segments and not isinstance(view, DatasetProfileView):
            raise ValueError(
                "You must pass either a DatasetProfile or a DatasetProfileView in order to use this writer!"
            )

        if _uncompound_metric_feature_flag():
            if has_segments:
                updated_profile_view = _uncompound_dataset_profile(view.profile_view)
                view = SegmentedDatasetProfileView(
                    profile_view=updated_profile_view, segment=view._segment, partition=view._partition
                )

            else:
                view = _uncompound_dataset_profile(view)

        if kwargs.get("dataset_id") is not None:
            self._dataset_id = kwargs.get("dataset_id")

        with tempfile.NamedTemporaryFile() as tmp_file:
            if has_segments:
                view.write(file=tmp_file, use_v0=True)
            else:
                view.write(file=tmp_file)
            tmp_file.flush()
            tmp_file.seek(0)
            utc_now = datetime.datetime.now(datetime.timezone.utc)
            dataset_timestamp = view.dataset_timestamp or utc_now
            stamp = dataset_timestamp.timestamp()
            time_delta_seconds = utc_now.timestamp() - stamp
            if time_delta_seconds < 0:
                logger.warning(
                    f"About to upload a profile with a dataset_timestamp that is in the future: {time_delta_seconds}s old."
                )
            elif time_delta_seconds > WEEK_IN_SECONDS:
                if time_delta_seconds > FIVE_YEARS_IN_SECONDS:
                    logger.error(
                        f"A profile being uploaded to WhyLabs has a dataset_timestamp of({dataset_timestamp}) "
                        f"compared to current datetime: {utc_now}. Uploads of profiles older than 5 years "
                        "might not be monitored in WhyLabs and may take up to 24 hours to show up."
                    )
                else:
                    logger.warning(
                        f"A profile being uploaded to WhyLabs has a dataset_timestamp of {dataset_timestamp} "
                        f"which is older than 7 days compared to {utc_now}. These profiles should be processed within 24 hours."
                    )

            if stamp <= 0:
                logger.error(
                    f"Profiles should have timestamps greater than 0, but found a timestamp of {stamp}"
                    f" and current timestamp is {utc_now.timestamp()}, this is likely an error."
                )

            dataset_timestamp_epoch = int(stamp * 1000)
            response = self._do_upload(
                dataset_timestamp=dataset_timestamp_epoch,
                profile_file=tmp_file,
            )
        # TODO: retry
        return response

    def _validate_api_key(self) -> None:
        if self._api_key is None:
            raise ValueError("Missing API key. Set it via WHYLABS_API_KEY environment variable or as an api_key option")
        if len(self._api_key) < 12:
            raise ValueError("API key too short")
        if len(self._api_key) > 64:
            raise ValueError("API key too long")
        if self._api_key[10] != ".":
            raise ValueError("Invalid format. Expecting a dot at an index 10")
        self._api_key_id = self._api_key[:10]

    def _validate_org_and_dataset(self) -> None:
        if self._org_id is None:
            raise EnvironmentError(
                "Missing organization ID. Specify it via option or WHYLABS_DEFAULT_ORG_ID " "environment variable"
            )
        if self._dataset_id is None:
            raise EnvironmentError(
                "Missing dataset ID. Specify it via WHYLABS_DEFAULT_DATASET_ID environment "
                "variable or on your write method"
            )

    def _do_get_feature_weights(self):
        """Get latest version for the feature weights for the specified dataset

        Returns
        -------
            Response of the GET request, with segmentWeights and metadata.
        """
        self._validate_org_and_dataset()
        self._validate_api_key()

        result = self._get_column_weights()
        return result

    def _do_upload_feature_weights(self) -> Tuple[bool, str]:
        """Put feature weights for the specified dataset.

        Returns
        -------
        Tuple[bool, str]
            Tuple with a boolean (1-success, 0-fail) and string with the request's status code.
        """

        self._validate_org_and_dataset()
        self._validate_api_key()

        result = self._put_feature_weights()
        if result == 200:
            return True, str(result)
        else:
            return False, str(result)

    def _copy_file(self, profile_file: IO[bytes], upload_url: str, dataset_timestamp: int, api_key_id: Optional[str]):
        http_response = requests.put(upload_url, data=profile_file.read())
        is_successful = False
        if http_response.status_code == 200:
            is_successful = True
            logger.info(
                f"Done uploading {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                f"{self.whylabs_api_endpoint} with API token ID: {api_key_id}"
            )
        return is_successful, http_response.text

    def _do_upload(
        self, dataset_timestamp: int, profile_path: Optional[str] = None, profile_file: Optional[IO[bytes]] = None
    ) -> Tuple[bool, str]:

        assert profile_path or profile_file, "Either a file or file path must be specified when uploading profiles"
        self._validate_org_and_dataset()
        self._validate_api_key()

        logger.debug("Generating the upload URL")
        upload_url = self._get_upload_url(dataset_timestamp=dataset_timestamp)
        api_key_id = self._api_key[:10] if self._api_key else None
        try:
            if profile_file:
                return self._copy_file(profile_file, upload_url, dataset_timestamp, api_key_id)
            elif profile_path:
                with open(profile_path, "rb") as f:
                    return self._copy_file(f, upload_url, dataset_timestamp, api_key_id)
        except requests.RequestException as e:
            logger.info(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                + f"{self.whylabs_api_endpoint} with API token ID: {api_key_id}. Error occurred: {e}"
            )
            return False, str(e)
        return False, "Either a profile_file or profile_path must be specified when uploading profiles to WhyLabs!"

    def _get_or_create_feature_weights_client(self) -> FeatureWeightsApi:
        environment_api_key = os.environ.get(API_KEY_ENV)

        if self._api_log_client is None:
            self._refresh_api_client()
        elif environment_api_key is not None and self._api_key != environment_api_key:
            logger.warning(f"Updating API key ID from: {self._api_key_id} to: {environment_api_key[:10]}")
            self._api_key = environment_api_key
            self._refresh_api_client()

        return FeatureWeightsApi(self._api_log_client)

    def _get_or_create_api_log_client(self) -> LogApi:
        environment_api_key = os.environ.get(API_KEY_ENV)

        if self._api_log_client is None:
            self._refresh_api_client()
        elif environment_api_key is not None and self._api_key != environment_api_key:
            logger.warning(f"Updating API key ID from: {self._api_key_id} to: {environment_api_key[:10]}")
            self._api_key = environment_api_key
            self._refresh_api_client()

        return LogApi(self._api_log_client)

    def _refresh_api_client(self) -> None:
        self._validate_api_key()

        if self._configuration is None:
            config = whylabs_client.Configuration()
        else:
            config = self._configuration
        config.ssl_ca_cert = self._ssl_ca_cert
        config.host = self.whylabs_api_endpoint
        config.api_key = {"ApiKeyAuth": self._api_key}
        config.discard_unknown_keys = True

        client = whylabs_client.ApiClient(config)
        client.user_agent = f"whylogs/python/{_version}"
        self._api_log_client = client

    def _get_rest_config(self) -> Configuration:
        if self._configuration is None:
            config = whylabs_client.Configuration()
        else:
            config = self._configuration

        config.host = self.whylabs_api_endpoint
        config.api_key = {"ApiKeyAuth": self._api_key}
        config.discard_unknown_keys = True
        return config

    @staticmethod
    def _build_log_async_request(dataset_timestamp: int) -> LogAsyncRequest:
        return LogAsyncRequest(dataset_timestamp=dataset_timestamp, segment_tags=[])

    @staticmethod
    def _build_log_reference_request(dataset_timestamp: int, alias: Optional[str] = None) -> LogReferenceRequest:
        return LogReferenceRequest(dataset_timestamp=dataset_timestamp, alias=alias)

    def _get_column_weights(self):
        feature_weight_api = self._get_or_create_feature_weights_client()
        try:
            result = feature_weight_api.get_column_weights(
                org_id=self._org_id,
                dataset_id=self._dataset_id,
            )
            return result
        except ForbiddenException as e:
            api_key_id = self._api_key[:10] if self._api_key else None
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {api_key_id}"
            )
            raise e

    def _put_feature_weights(self):
        feature_weight_api = self._get_or_create_feature_weights_client()
        try:
            result = feature_weight_api.put_column_weights(
                org_id=self._org_id,
                dataset_id=self._dataset_id,
                body={
                    "segmentWeights": [self._feature_weights],
                },
                _return_http_data_only=False,
            )
            return result[1]
        except ForbiddenException as e:
            api_key_id = self._api_key[:10] if self._api_key else None
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {api_key_id}"
            )
            raise e

    def _post_log_async(self, request: LogAsyncRequest, dataset_timestamp: int) -> AsyncLogResponse:
        log_api = self._get_or_create_api_log_client()
        try:
            result = log_api.log_async(org_id=self._org_id, dataset_id=self._dataset_id, log_async_request=request)
            return result
        except ForbiddenException as e:
            api_key_id = self._api_key[:10] if self._api_key else None
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {api_key_id}"
            )
            raise e

    def _post_log_reference(self, request: LogAsyncRequest, dataset_timestamp: int) -> LogReferenceResponse:
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
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {api_key_id}"
            )
            raise e

    def _get_upload_url(self, dataset_timestamp: int) -> str:
        if self._reference_profile_name is not None:
            request = self._build_log_reference_request(dataset_timestamp, alias=self._reference_profile_name)
            res = self._post_log_reference(request=request, dataset_timestamp=dataset_timestamp)
            upload_url = res["upload_url"]
        else:
            request = self._build_log_async_request(dataset_timestamp)
            res = self._post_log_async(request=request, dataset_timestamp=dataset_timestamp)
            upload_url = res["upload_url"]
        return upload_url
