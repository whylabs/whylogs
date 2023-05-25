import abc
import copy
import datetime
import logging
import os
import tempfile
from typing import IO, Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import requests  # type: ignore
import whylabs_client  # type: ignore
from urllib3 import PoolManager, ProxyManager
from whylabs_client import ApiClient, Configuration  # type: ignore
from whylabs_client.api.dataset_profile_api import DatasetProfileApi
from whylabs_client.api.feature_weights_api import FeatureWeightsApi
from whylabs_client.api.log_api import AsyncLogResponse  # type: ignore
from whylabs_client.api.log_api import (
    LogApi,
    LogAsyncRequest,
    LogReferenceRequest,
    LogReferenceResponse,
)
from whylabs_client.api.models_api import ModelsApi
from whylabs_client.model.column_schema import ColumnSchema
from whylabs_client.model.create_reference_profile_request import (
    CreateReferenceProfileRequest,
)
from whylabs_client.model.segment import Segment
from whylabs_client.model.segment_tag import SegmentTag
from whylabs_client.rest import ForbiddenException  # type: ignore

from whylogs import __version__ as _version
from whylogs.api.logger import log
from whylogs.api.logger.result_set import SegmentedResultSet
from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.core import DatasetProfileView
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.errors import BadConfigError
from whylogs.core.feature_weights import FeatureWeights
from whylogs.core.utils import deprecated_alias
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView
from whylogs.experimental.performance_estimation.estimation_results import (
    EstimationResult,
)
from whylogs.migration.converters import _generate_segment_tags_metadata
from whylogs.migration.uncompound import (
    FeatureFlags,
    _uncompound_dataset_profile,
    _uncompound_metric_feature_flag,
    _uncompound_performance_estimation_feature_flag,
    _uncompound_performance_estimation_magic_string,
)

FIVE_MINUTES_IN_SECONDS = 60 * 5
DAY_IN_SECONDS = 60 * 60 * 24
WEEK_IN_SECONDS = DAY_IN_SECONDS * 7
FIVE_YEARS_IN_SECONDS = DAY_IN_SECONDS * 365 * 5
logger = logging.getLogger(__name__)

API_KEY_ENV = "WHYLABS_API_KEY"

_API_CLIENT_CACHE: Dict[str, ApiClient] = dict()
_UPLOAD_POOLER_CACHE: Dict[str, Union[PoolManager, ProxyManager]] = dict()

_US_WEST2_DOMAIN = "songbird-20201223060057342600000001.s3.us-west-2.amazonaws.com"
_S3_PUBLIC_DOMAIN = os.environ.get("_WHYLABS_PRIVATE_S3_DOMAIN") or _US_WEST2_DOMAIN
_WHYLABS_SKIP_CONFIG_READ = os.environ.get("_WHYLABS_SKIP_CONFIG_READ") or False


def _check_whylabs_condition_count_uncompound() -> bool:
    global _WHYLABS_SKIP_CONFIG_READ
    if _WHYLABS_SKIP_CONFIG_READ:
        return True
    whylabs_config_url = (
        "https://whylabs-public.s3.us-west-2.amazonaws.com/whylogs_config/whylabs_condition_count_disabled"
    )
    logger.info(f"checking: {whylabs_config_url}")
    try:
        response = requests.head(whylabs_config_url)
        logger.info(f"checking: {whylabs_config_url}")
        logger.info(f"headers are: {response.headers} code: {response.status_code}")
        if response.status_code == 200:
            logger.info(
                "found the whylabs condition count disabled file so running uncompound on condition count metrics"
            )
            return True
        elif response.status_code == 404:
            logger.info("no whylabs condition count disabled so sending condition count metrics as v1.")
            return False
        else:
            logger.info(f"Got response code {response.status_code} but expected 200, so running uncompound")
    except Exception:
        logger.warning("Error trying to read whylabs config, falling back to defaults for uncompounding")
    _WHYLABS_SKIP_CONFIG_READ = True
    return True


def _validate_api_key(api_key: Optional[str]) -> str:
    if api_key is None:
        raise ValueError("Missing API key. Set it via WHYLABS_API_KEY environment variable or as an api_key option")
    if len(api_key) < 12:
        raise ValueError("API key too short")
    if len(api_key) > 64:
        raise ValueError("API key too long")
    if api_key[10] != ".":
        raise ValueError("Invalid format. Expecting a dot at an index 10")
    return api_key[:10]


class KeyRefresher(abc.ABC):
    @property
    @abc.abstractmethod
    def key_id(self) -> str:
        pass

    @abc.abstractmethod
    def __call__(self, config: Configuration) -> None:
        pass


class StaticKeyRefresher(KeyRefresher):
    def __init__(self, api_key: str) -> None:
        self._key_id = _validate_api_key(api_key)
        self._api_key = api_key

    @property
    def key_id(self) -> str:
        return self._key_id

    def __call__(self, config: Configuration) -> None:
        config.api_key = {"ApiKeyAuth": self._api_key}

    def __hash__(self):
        return hash(self._api_key)


class EnvironmentKeyRefresher(KeyRefresher):
    """
    This key refresher uses environment variable key. The key is automatically picked up if the
    user changes the environment variable.
    """

    @property
    def key_id(self) -> str:
        return self._key_id

    def __call__(self, config: Configuration) -> None:
        api_key = os.environ.get(API_KEY_ENV)
        self._key_id = _validate_api_key(api_key)
        assert api_key is not None
        config.api_key = {"ApiKeyAuth": api_key}


_ENV_KEY_REFRESHER = EnvironmentKeyRefresher()


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

    _key_refresher: KeyRefresher
    _endpoint_hostname: Optional[str] = None
    _s3_private_domain: Optional[str] = None
    _s3_endpoint_subject: Optional[str] = None
    _timeout_seconds: float = 300.0

    def __init__(
        self,
        org_id: Optional[str] = None,
        api_key: Optional[str] = None,
        dataset_id: Optional[str] = None,
        api_client: Optional[ApiClient] = None,
        ssl_ca_cert: Optional[str] = None,
        _timeout_seconds: Optional[float] = None,
    ):
        self._org_id = org_id or os.environ.get("WHYLABS_DEFAULT_ORG_ID")
        self._dataset_id = dataset_id or os.environ.get("WHYLABS_DEFAULT_DATASET_ID")
        self._feature_weights = None
        self.whylabs_api_endpoint = os.environ.get("WHYLABS_API_ENDPOINT") or "https://api.whylabsapp.com"
        self._reference_profile_name = os.environ.get("WHYLABS_REFERENCE_PROFILE_NAME")
        self._ssl_ca_cert = ssl_ca_cert
        self._api_config: Optional[Configuration] = None

        if api_key:
            self._key_refresher = StaticKeyRefresher(api_key)
        else:
            self._key_refresher = _ENV_KEY_REFRESHER
        if api_client:
            self._api_client = api_client
        else:
            self._api_client = None
            self._refresh_client()

        # Enable private access to WhyLabs endpoints
        _private_api_endpoint = os.environ.get("WHYLABS_PRIVATE_API_ENDPOINT")
        _private_s3_endpoint = os.environ.get("WHYLABS_PRIVATE_S3_ENDPOINT")
        if _private_api_endpoint:
            logger.debug(f"Using private API endpoint: {_private_api_endpoint}")
            self._endpoint_hostname = urlparse(self.whylabs_api_endpoint).netloc
            self.whylabs_api_endpoint = _private_api_endpoint

        pooler_cache_key = ""
        if _private_s3_endpoint:
            logger.debug(f"Using private S3 endpoint: {_private_s3_endpoint}")
            self._s3_private_domain = urlparse(_private_s3_endpoint).netloc
            self._s3_endpoint_subject = _S3_PUBLIC_DOMAIN
            pooler_cache_key += self._s3_private_domain

        if _timeout_seconds is not None:
            self._timeout_seconds = _timeout_seconds

        # Using a pooler for uploading data
        pool = _UPLOAD_POOLER_CACHE.get(pooler_cache_key)
        if pool is None:
            logger.debug(f"Pooler is not available. Creating a new one for key: {pooler_cache_key}")
            pool = PoolManager(
                num_pools=4,
                maxsize=10,
                assert_hostname=self._s3_endpoint_subject,
                server_hostname=self._s3_endpoint_subject,
            )
            self._s3_pool = pool
            _UPLOAD_POOLER_CACHE[pooler_cache_key] = pool
        else:
            self._s3_pool = pool

    @property
    def key_id(self) -> str:
        return self._key_refresher.key_id

    def _refresh_client(self) -> None:
        """
        Refresh the API client by comparing various configs. We try to
        re-use the client as much as we can since using a new client
        every time can be expensive.

        """
        cache_key = ""

        if self._api_client:
            config = copy.deepcopy(self._api_client.configuration)
        else:
            config = Configuration()
        # Set an empty api key. The key refresher will refresh it
        config.api_key = {"ApiKeyAuth": ""}
        config.refresh_api_key_hook = self._key_refresher
        cache_key += str(hash(self._key_refresher))

        config.discard_unknown_keys = True
        # Disable client side validation and trust the server
        config.client_side_validation = False

        cache_key += str(hash(config))
        if self._ssl_ca_cert:
            config.ssl_ca_cert = self._ssl_ca_cert
            cache_key += str(hash(self._ssl_ca_cert))
        config.host = self.whylabs_api_endpoint
        cache_key += str(hash(self.whylabs_api_endpoint))
        if self._endpoint_hostname:
            cache_key += str(hash(self._endpoint_hostname))

        existing_client = _API_CLIENT_CACHE.get(cache_key)
        if existing_client:
            logger.debug(f"Found existing client under cache key: {cache_key}")
            self._api_client = existing_client
            return

        client = whylabs_client.ApiClient(config)
        client.user_agent = f"whylogs/python/{_version}"

        self._api_client = client
        _API_CLIENT_CACHE[cache_key] = client
        logger.debug(f"Created and updated new client for cache key: {cache_key}")

        if self._endpoint_hostname:
            logger.info(f"Override endpoint hostname for TLS verification is set to: {self._endpoint_hostname}")
            self._update_hostname_config(self._endpoint_hostname)

    def _update_hostname_config(self, endpoint_hostname_override: str) -> None:
        """
        This method overrides the pool manager's new connection method to add the hostname

        """
        import urllib3

        if isinstance(self._api_client.rest_client.pool_manager, urllib3.ProxyManager):
            raise ValueError("Endpoint hostname override is not supported when using with proxy")

        logger.debug(f"Override endpoint hostname to: {endpoint_hostname_override}")
        old_conn_factory = self._api_client.rest_client.pool_manager.connection_from_host

        def new_conn_factory(host: str, port: int, scheme: str, pool_kwargs: Optional[Dict[str, str]] = None) -> Any:
            if pool_kwargs is None:
                pool_kwargs = {}
            pool_kwargs["assert_hostname"] = endpoint_hostname_override
            pool_kwargs["server_hostname"] = endpoint_hostname_override
            return old_conn_factory(host, port, scheme, pool_kwargs)

        self._api_client.rest_client.pool_manager.connection_from_host = new_conn_factory

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
        api_client: Optional[ApiClient] = None,
        timeout_seconds: Optional[float] = None,
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
            self._key_refresher = StaticKeyRefresher(api_key)
            self._refresh_client()
        if reference_profile_name is not None:
            self._reference_profile_name = reference_profile_name
        if configuration is not None:
            raise ValueError("Manual configuration is not supported. Please override the api_client instead")
        if api_client is not None:
            self._api_client = api_client
        if ssl_ca_cert is not None:
            self._ssl_ca_cert = ssl_ca_cert
            self._refresh_client()
        if timeout_seconds is not None:
            self._timeout_seconds = timeout_seconds
        return self

    def _tag_columns(self, columns: List[str], value: str) -> Tuple[bool, str]:
        """Sets the column as an input or output for the specified dataset.

        Parameters
        ----------
        columns : List[str]
            The list of column names you want to tag as input or output

        value: str
            For example we can pass in "output" to configure the column as an output.

        Returns
        -------
        Tuple[bool, str]
            Tuple with a boolean indicating success or failure: e.g. (True, "column prediction was updated to
            output") and string with status message.
        """
        self._validate_org_and_dataset()
        results: Dict[str, str] = dict()
        all_sucessful = True
        # TODO: update the list of columns at once, support arbitrary tags as well.
        for column_name in columns:
            status = self._put_column_schema(column_name, value)
            results[column_name] = status[1]
            if status[0] != 200:
                all_sucessful = False
        return all_sucessful, str(results)

    def tag_output_columns(self, columns: List[str]) -> Tuple[bool, str]:
        """Sets the list of columns as output columns for the specified dataset.

        Parameters
        ----------
        columns : List[str]
            The list of column names you want to tag as outputs.

        Returns
        -------
        Tuple[bool, str]
            Tuple with a boolean indicating success or failure: e.g. (True, "column prediction was updated to
            output") and string with status message.
        """

        return self._tag_columns(columns, "output")

    def tag_input_columns(self, columns: List[str]) -> Tuple[bool, str]:
        """Sets the list of columns as input columns for the specified dataset.

        Parameters
        ----------
        columns : List[str]
            The list of column names you want to tag as inputs.

        Returns
        -------
        Tuple[bool, str]
            Tuple with a boolean indicating success or failure: e.g. (True, "column [output_voltage] updated to
            input") and string with status message.
        """

        return self._tag_columns(columns, "input")

    def write_estimation_result(self, file: EstimationResult, **kwargs: Any) -> Tuple[bool, str]:
        if _uncompound_performance_estimation_feature_flag():
            estimation_magic_string = _uncompound_performance_estimation_magic_string()
            estimation_result_profile = log({f"{estimation_magic_string}accuracy": file.accuracy}).profile()
            estimation_result_profile.set_dataset_timestamp(file.target_result_timestamp)
            return self.write(estimation_result_profile.view())
        return False, str("Performance estimation feature flag is not enabled")

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
        feature_weights_set = result.get("segment_weights")
        metadata = result.get("metadata")
        if feature_weights_set and isinstance(feature_weights_set, list):
            feature_weights = FeatureWeights(weights=feature_weights_set[0]["weights"], metadata=metadata)
            return feature_weights
        return None

    def _write_segmented_reference_result_set(self, file: SegmentedResultSet, **kwargs: Any) -> Tuple[bool, str]:
        """Put segmented reference result set for the specified dataset.

        Parameters
        ----------
        file : SegmentedResultSet
            SegmentedResultSet object representing the segmented reference result set for the specified dataset

        Returns
        -------
        Tuple[bool, str]
        """
        utc_now = datetime.datetime.now(datetime.timezone.utc)

        files = file.get_writables()
        partitions = file.partitions
        if len(partitions) > 1:
            logger.warning(
                "SegmentedResultSet contains more than one partition. Only the first partition will be uploaded. "
            )
        partition = partitions[0]
        whylabs_tags = list()
        for view in files:
            view_tags = list()
            dataset_timestamp = view.dataset_timestamp or utc_now
            if view.partition.id != partition.id:
                continue
            _, segment_tags, _ = _generate_segment_tags_metadata(view.segment, view.partition)
            for segment_tag in segment_tags:
                tag_key = segment_tag.key.replace("whylogs.tag.", "")
                tag_value = segment_tag.value
                view_tags.append({"key": tag_key, "value": tag_value})
            whylabs_tags.append(view_tags)
        stamp = dataset_timestamp.timestamp()
        dataset_timestamp_epoch = int(stamp * 1000)
        profile_id, upload_urls = self._get_upload_urls_segmented_reference(whylabs_tags, dataset_timestamp_epoch)
        upload_statuses = list()
        for view, url in zip(files, upload_urls):
            with tempfile.NamedTemporaryFile() as tmp_file:
                if kwargs.get("use_v0") is None or kwargs.get("use_v0"):
                    view.write(file=tmp_file, use_v0=True)
                else:
                    view.write(file=tmp_file)
                tmp_file.flush()
                tmp_file.seek(0)

                upload_res = self._do_upload(
                    dataset_timestamp=dataset_timestamp_epoch,
                    upload_url=url,
                    profile_id=profile_id,
                    profile_file=tmp_file,
                )
                upload_statuses.append(upload_res)
        if all([status[0] for status in upload_statuses]):
            return upload_statuses[0]
        else:
            return False, "Failed to upload all segments"

    @deprecated_alias(profile="file")
    def write(self, file: Writable, **kwargs: Any) -> Tuple[bool, str]:
        if isinstance(file, FeatureWeights):
            return self.write_feature_weights(file, **kwargs)
        elif isinstance(file, EstimationResult):
            return self.write_estimation_result(file, **kwargs)
        elif isinstance(file, SegmentedResultSet) and self._reference_profile_name is not None:
            return self._write_segmented_reference_result_set(file, **kwargs)
        view = file.view() if isinstance(file, DatasetProfile) else file
        has_segments = isinstance(view, SegmentedDatasetProfileView)

        if not has_segments and not isinstance(view, DatasetProfileView):
            raise ValueError(
                "You must pass either a DatasetProfile or a DatasetProfileView in order to use this writer!"
            )

        flags = FeatureFlags(_check_whylabs_condition_count_uncompound())

        if _uncompound_metric_feature_flag():
            if has_segments:
                updated_profile_view = _uncompound_dataset_profile(view.profile_view, flags)
                view = SegmentedDatasetProfileView(
                    profile_view=updated_profile_view, segment=view._segment, partition=view._partition
                )

            else:
                view = _uncompound_dataset_profile(view, flags)

        if kwargs.get("dataset_id") is not None:
            self._dataset_id = kwargs.get("dataset_id")

        with tempfile.NamedTemporaryFile() as tmp_file:
            # currently whylabs is not ingesting the v1 format of segmented profiles as segmented
            # so we default to sending them as v0 profiles if the override `use_v0` is not defined,
            # if `use_v0` is defined then pass that through to control the serialization format.
            if has_segments and (kwargs.get("use_v0") is None or kwargs.get("use_v0")):
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
                    f"About to upload a profile with a dataset_timestamp that is in the future: "
                    f"{time_delta_seconds}s old."
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
                        f"which is older than 7 days compared to {utc_now}. These profiles should be processed "
                        f"within 24 hours."
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

        result = self._put_feature_weights()
        if result == 200:
            return True, str(result)
        else:
            return False, str(result)

    def _put_file(self, profile_file: IO[bytes], upload_url: str, dataset_timestamp: int) -> Tuple[bool, str]:
        # TODO: probably want to call this API using asyncio
        headers = {"Content-Type": "application/octet-stream"}
        if self._s3_endpoint_subject:
            logger.info(f"Override Host parameter since we are using S3 private endpoint: {self._s3_private_domain}")
            headers["Host"] = self._s3_endpoint_subject
        response = self._s3_pool.request(
            "PUT", upload_url, headers=headers, timeout=self._timeout_seconds, body=profile_file.read()
        )
        is_successful = False
        if response.status == 200:
            is_successful = True
            logger.info(
                f"Done uploading {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                f"{self.whylabs_api_endpoint} with API token ID: {self._key_refresher.key_id}"
            )
        return is_successful, response.reason

    def _do_upload(
        self,
        dataset_timestamp: int,
        upload_url: Optional[str] = None,
        profile_id: Optional[str] = None,
        profile_path: Optional[str] = None,
        profile_file: Optional[IO[bytes]] = None,
    ) -> Tuple[bool, str]:
        assert profile_path or profile_file, "Either a file or file path must be specified when uploading profiles"
        self._validate_org_and_dataset()

        # logger.debug("Generating the upload URL")
        if upload_url and not profile_id:
            raise ValueError("If upload_url is specified, profile_id must also be specified")
        elif profile_id and not upload_url:
            raise ValueError("If profile_id is specified, upload_url must also be specified")
        elif not upload_url and not profile_id:
            upload_url, profile_id = self._get_upload_url(dataset_timestamp=dataset_timestamp)
        try:
            if profile_file:
                status, reason = self._put_file(profile_file, upload_url, dataset_timestamp)  # type: ignore
                logger.debug(f"copied file {upload_url} status {status}:{reason}")
                return status, profile_id  # type: ignore
            elif profile_path:
                with open(profile_path, "rb") as f:
                    status, reason = self._put_file(f, upload_url, dataset_timestamp)  # type: ignore
                    logger.debug(f"copied file {upload_url} status {status}:{reason}")
                    return status, profile_id  # type: ignore
        except requests.RequestException as e:
            logger.info(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                + f"{self.whylabs_api_endpoint} with API token ID: {self.key_id}. Error occurred: {e}"
            )
            return False, str(e)
        return False, "Either a profile_file or profile_path must be specified when uploading profiles to WhyLabs!"

    def _get_or_create_feature_weights_client(self) -> FeatureWeightsApi:
        return FeatureWeightsApi(self._api_client)

    def _get_or_create_models_client(self) -> ModelsApi:
        return ModelsApi(self._api_client)

    def _get_or_create_api_log_client(self) -> LogApi:
        return LogApi(self._api_client)

    def _get_or_create_api_dataset_client(self) -> DatasetProfileApi:
        return DatasetProfileApi(self._api_client)

    @staticmethod
    def _build_log_async_request(dataset_timestamp: int) -> LogAsyncRequest:
        return LogAsyncRequest(dataset_timestamp=dataset_timestamp, segment_tags=[])

    @staticmethod
    def _build_log_reference_request(dataset_timestamp: int, alias: Optional[str] = None) -> LogReferenceRequest:
        return LogReferenceRequest(dataset_timestamp=dataset_timestamp, alias=alias)

    @staticmethod
    def _build_log_segmented_reference_request(
        dataset_timestamp: int, tags: Optional[dict] = None, alias: Optional[str] = None
    ) -> LogReferenceRequest:
        segments = list()
        if tags is not None:
            for segment_tags in tags:
                segments.append(Segment(tags=[SegmentTag(key=tag["key"], value=tag["value"]) for tag in segment_tags]))
        if not segments:
            return CreateReferenceProfileRequest(alias=alias, dataset_timestamp=dataset_timestamp)
        else:
            return CreateReferenceProfileRequest(alias=alias, dataset_timestamp=dataset_timestamp, segments=segments)

    def _get_column_weights(self):
        feature_weight_api = self._get_or_create_feature_weights_client()
        try:
            result = feature_weight_api.get_column_weights(
                org_id=self._org_id,
                dataset_id=self._dataset_id,
            )
            return result
        except ForbiddenException as e:
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {self.key_id}"
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
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {self.key_id}"
            )
            raise e

    def _get_existing_column_schema(self, model_api_instance, column_name) -> Optional[ColumnSchema]:
        try:
            # TODO: remove when whylabs supports merge writes.
            existing_schema = model_api_instance.get_entity_schema_column(
                org_id=self._org_id, dataset_id=self._dataset_id, column_id=column_name
            )

            if existing_schema:
                data_type = existing_schema.get("dataType")
                discreteness = existing_schema.get("discreteness")
                existing_classification = existing_schema.get("classifier")
                column_schema = ColumnSchema(
                    classifier=existing_classification, data_type=data_type, discreteness=discreteness
                )
                return column_schema
        except ForbiddenException as e:
            logger.exception(
                f"Failed to set column outputs {self._org_id}/{self._dataset_id} for column name: ({column_name}) "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {self.key_id}"
            )
            raise e
        return None

    def _column_schema_needs_update(self, column_schema: ColumnSchema, new_classification: str) -> bool:
        existing_classification = column_schema.classifier
        if not existing_classification:
            return True
        return existing_classification != new_classification

    def _put_column_schema(self, column_name: str, value: str) -> Tuple[int, str]:
        model_api_instance = self._get_or_create_models_client()

        # TODO: simplify after ColumnSchema updates support merge writes, can remove the read here.
        column_schema: ColumnSchema = self._get_existing_column_schema(model_api_instance, column_name)
        if self._column_schema_needs_update(column_schema, value):
            updated_column_schema = ColumnSchema(
                classifier=value, data_type=column_schema.data_type, discreteness=column_schema.discreteness
            )
            try:
                # TODO: remove when whylabs supports merge writes.
                model_api_instance.put_entity_schema_column(
                    self._org_id, self._dataset_id, column_name, updated_column_schema
                )
                return (
                    200,
                    f"{column_name} updated from {column_schema.classifier} to {updated_column_schema.classifier}",
                )
            except ForbiddenException as e:
                logger.exception(
                    f"Failed to set column outputs {self._org_id}/{self._dataset_id} for column name: ("
                    f"{column_name}) "
                    f"{self.whylabs_api_endpoint}"
                    f" with API token ID: {self.key_id}"
                )
                raise e
        else:
            no_update_made_message = f"column {column_name} was already classified {column_schema.classifier}."
            logger.info(no_update_made_message)
            return (200, no_update_made_message)

    def _post_log_async(self, request: LogAsyncRequest, dataset_timestamp: int) -> AsyncLogResponse:
        log_api = self._get_or_create_api_log_client()
        try:
            result = log_api.log_async(org_id=self._org_id, dataset_id=self._dataset_id, log_async_request=request)
            return result
        except ForbiddenException as e:
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {self.key_id}"
            )
            raise e

    def _get_upload_urls_segmented_reference(self, whylabs_tags, dataset_timestamp: int) -> Tuple[str, List[str]]:
        request = self._build_log_segmented_reference_request(
            dataset_timestamp, tags=whylabs_tags, alias=self._reference_profile_name
        )
        res = self._post_log_segmented_reference(request=request, dataset_timestamp=dataset_timestamp)
        return res["id"], res["upload_urls"]

    def _post_log_segmented_reference(self, request: LogAsyncRequest, dataset_timestamp: int) -> LogReferenceResponse:
        dataset_api = self._get_or_create_api_dataset_client()
        try:
            async_result = dataset_api.create_reference_profile(
                org_id=self._org_id,
                dataset_id=self._dataset_id,
                create_reference_profile_request=request,
                async_req=True,
            )

            result = async_result.get()
            return result
        except ForbiddenException as e:
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {self.key_id}"
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
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {self.key_id}"
            )
            raise e

    def _get_upload_url(self, dataset_timestamp: int) -> Tuple[str, str]:
        if self._reference_profile_name is not None:
            request = self._build_log_reference_request(dataset_timestamp, alias=self._reference_profile_name)
            res = self._post_log_reference(request=request, dataset_timestamp=dataset_timestamp)
        else:
            request = self._build_log_async_request(dataset_timestamp)
            res = self._post_log_async(request=request, dataset_timestamp=dataset_timestamp)

        upload_url = res["upload_url"]
        profile_id = res["id"]

        if self._s3_private_domain:
            if _S3_PUBLIC_DOMAIN not in upload_url:
                raise ValueError(
                    "S3 private domain is enabled but your account is not using S3 upload endpoint. " "Aborting!"
                )
            upload_url = upload_url.replace(_S3_PUBLIC_DOMAIN, self._s3_private_domain)
            logger.debug(f"Replaced URL with our private domain. New URL: {upload_url}")

        return upload_url, profile_id
