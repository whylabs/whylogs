import datetime
import logging
import os
import tempfile
from typing import Any, Dict, Optional

import requests  # type: ignore
import whylabs_client  # type: ignore
from whylabs_client import ApiClient, Configuration  # type: ignore
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
from whylogs.core import ColumnProfileView, DatasetProfileView
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.errors import BadConfigError
from whylogs.core.metrics import Metric
from whylogs.core.metrics.compound_metric import CompoundMetric
from whylogs.core.utils import deprecated_alias
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView

try:
    from PIL.Image import Image as ImageType  # noqa trigger ImportError if unavailable

    from whylogs.extras.image_metric import ImageMetric
except ImportError:
    # dummy ImageMetric class so nothing will be an instance of it
    class ImageMetric:  # type: ignore[no-redef]
        pass


FIVE_MINUTES_IN_SECONDS = 60 * 5
logger = logging.getLogger(__name__)

API_KEY_ENV = "WHYLABS_API_KEY"


def _uncompound_metric_feature_flag() -> bool:
    return True


def _v0_compatible_image_feature_flag() -> bool:
    return False


def _uncompounded_column_name(column_name: str, metric_name: str, submetric_name: str, metric: Metric) -> str:
    if isinstance(metric, ImageMetric) and _v0_compatible_image_feature_flag():
        return submetric_name
    return f"{column_name}.{metric_name}.{submetric_name}"


def _uncompound_metric(col_name: str, metric_name: str, metric: CompoundMetric) -> Dict[str, ColumnProfileView]:
    result: Dict[str, ColumnProfileView] = dict()
    for submetric_name, submetric in metric.submetrics.items():
        new_col_name = _uncompounded_column_name(col_name, metric_name, submetric_name, metric)
        result[new_col_name] = ColumnProfileView({submetric.namespace: submetric})
    return result


def _uncompund_dataset_profile(prof: DatasetProfileView) -> DatasetProfileView:
    new_prof = DatasetProfileView(
        columns=prof._columns,
        dataset_timestamp=prof._dataset_timestamp,
        creation_timestamp=prof._creation_timestamp,
        metrics=prof._metrics,
    )
    new_columns: Dict[str, ColumnProfileView] = dict()
    for col_name, col_prof in new_prof._columns.items():
        for metric_name, metric in col_prof._metrics.items():
            if isinstance(metric, CompoundMetric):
                new_columns.update(_uncompound_metric(col_name, metric_name, metric))

    new_prof._columns.update(new_columns)
    return new_prof


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

    @deprecated_alias(profile="file")
    def write(self, file: Writable, **kwargs: Any) -> None:
        view = file.view() if isinstance(file, DatasetProfile) else file
        has_segments = isinstance(view, SegmentedDatasetProfileView)

        if not has_segments and not isinstance(view, DatasetProfileView):
            raise ValueError(
                "You must pass either a DatasetProfile or a DatasetProfileView in order to use this writer!"
            )

        if _uncompound_metric_feature_flag():
            if has_segments:
                updated_profile_view = _uncompund_dataset_profile(view.profile_view)
                view = SegmentedDatasetProfileView(
                    profile_view=updated_profile_view, segment=view._segment, partition=view._partition
                )

            else:
                view = _uncompund_dataset_profile(view)

        if kwargs.get("dataset_id") is not None:
            self._dataset_id = kwargs.get("dataset_id")

        with tempfile.NamedTemporaryFile() as tmp_file:
            if has_segments:
                view.write(path=tmp_file.name, use_v0=True)
            else:
                view.write(path=tmp_file.name)
            tmp_file.flush()

            dataset_timestamp = view.dataset_timestamp or datetime.datetime.now(datetime.timezone.utc)
            dataset_timestamp_epoch = int(dataset_timestamp.timestamp() * 1000)
            self._do_upload(
                dataset_timestamp=dataset_timestamp_epoch,
                profile_path=tmp_file.name,
            )

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

    def _do_upload(
        self,
        dataset_timestamp: int,
        profile_path: str,
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
        if self._api_key is None:
            raise EnvironmentError("Missing API key. Specify it via WHYLABS_API_KEY environment variable.")

        logger.debug("Generating the upload URL")
        upload_url = self._get_upload_url(dataset_timestamp=dataset_timestamp)
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
        if self._reference_profile_name is None:
            request = self._build_log_async_request(dataset_timestamp)
            res = self._post_log_async(request=request, dataset_timestamp=dataset_timestamp)
            upload_url = res["upload_url"]
        else:
            request = self._build_log_reference_request(dataset_timestamp, alias=self._reference_profile_name)
            res = self._post_log_reference(request=request, dataset_timestamp=dataset_timestamp)
            upload_url = res["upload_url"]
        return upload_url
