import datetime
import logging
import os
import tempfile
from typing import IO, Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import requests  # type: ignore
from urllib3 import PoolManager, ProxyManager
from whylabs_client import ApiClient, Configuration  # type: ignore
from whylabs_client.api.dataset_profile_api import DatasetProfileApi  # type: ignore
from whylabs_client.api.feature_weights_api import FeatureWeightsApi  # type: ignore
from whylabs_client.api.log_api import AsyncLogResponse  # type: ignore
from whylabs_client.api.log_api import (
    LogApi,
    LogAsyncRequest,
    LogReferenceRequest,
    LogReferenceResponse,
)
from whylabs_client.api.models_api import ModelsApi  # type: ignore
from whylabs_client.model.column_schema import ColumnSchema  # type: ignore
from whylabs_client.model.create_reference_profile_request import (  # type: ignore
    CreateReferenceProfileRequest,
)
from whylabs_client.model.metric_schema import MetricSchema  # type: ignore
from whylabs_client.model.segment import Segment  # type: ignore
from whylabs_client.model.segment_tag import SegmentTag  # type: ignore
from whylabs_client.rest import ForbiddenException  # type: ignore

from whylogs.api.logger import log
from whylogs.api.logger.result_set import ResultSet, SegmentedResultSet
from whylogs.api.whylabs.session.session_manager import _INIT_DOCS, _default_init
from whylogs.api.whylabs.session.whylabs_client_cache import (
    ClientCacheConfig,
    EnvironmentKeyRefresher,
    KeyRefresher,
    WhylabsClientCache,
)
from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.context.environ import read_bool_env_var
from whylogs.core import DatasetProfileView
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.errors import BadConfigError
from whylogs.core.feature_weights import FeatureWeights
from whylogs.core.utils import deprecated_alias
from whylogs.core.utils.utils import get_auth_headers
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
FIVE_YEARS_IN_SECONDS = DAY_IN_SECONDS * 365 * 5
logger = logging.getLogger(__name__)
WHYLOGS_PREFER_SYNC_KEY = "WHYLOGS_PREFER_SYNC"

_API_CLIENT_CACHE: Dict[str, ApiClient] = dict()
_UPLOAD_POOLER_CACHE: Dict[str, Union[PoolManager, ProxyManager]] = dict()

_US_WEST2_DOMAIN = "songbird-20201223060057342600000001.s3.us-west-2.amazonaws.com"
_S3_PUBLIC_DOMAIN = os.environ.get("_WHYLABS_PRIVATE_S3_DOMAIN") or _US_WEST2_DOMAIN
_WHYLABS_SKIP_CONFIG_READ = os.environ.get("_WHYLABS_SKIP_CONFIG_READ") or False

KNOWN_CUSTOM_PERFORMANCE_METRICS = {
    "mean_average_precision_k_": "mean",
    "accuracy_k_": "mean",
    "mean_reciprocal_rank": "mean",
    "precision_k_": "mean",
    "recall_k_": "mean",
    "top_rank": "mean",
    "average_precision_k_": "mean",
}


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


class WhyLabsWriter(Writer):
    f"""
    A WhyLogs writer to upload DatasetProfileView's onto the WhyLabs platform.

    >**IMPORTANT**: In order to correctly send your profiles over, make sure you have
    the following environment variables set: `[WHYLABS_ORG_ID, WHYLABS_API_KEY, WHYLABS_DEFAULT_DATASET_ID]`. You
    can also follow the authentication instructions for the why.init() method at {_INIT_DOCS}.
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
    import whylogs as why

    why.init()

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
        session = _default_init()  # Force an init if the user didn't do it, it's idempotent
        config = session.config

        self._org_id = org_id or config.get_org_id()
        self._dataset_id = dataset_id or config.get_default_dataset_id()
        self._api_key = api_key or config.get_api_key()
        self._feature_weights = None
        self._reference_profile_name = config.get_whylabs_refernce_profile_name()
        self._api_config: Optional[Configuration] = None
        self._prefer_sync = read_bool_env_var(WHYLOGS_PREFER_SYNC_KEY, False)

        _http_proxy = os.environ.get("HTTP_PROXY")
        _https_proxy = os.environ.get("HTTPS_PROXY")
        self._proxy = _https_proxy or _http_proxy

        # Enable private access to WhyLabs endpoints
        _private_api_endpoint = config.get_whylabs_private_api_endpoint()
        _whylabs_endpoint = config.get_whylabs_endpoint()
        # TODO everything is incoherant when a client is supplied because all of these other variables are ignored,
        # the custom client should probably just be a parameter of write() and never be stored, or all of this other state
        # needs to be abstracted into some other container
        self.whylabs_api_endpoint = _private_api_endpoint or _whylabs_endpoint

        _private_s3_endpoint = config.get_whylabs_private_s3_endpoint()
        if _private_api_endpoint:
            logger.debug(f"Using private API endpoint: {_private_api_endpoint}")
            self._endpoint_hostname = urlparse(self.whylabs_api_endpoint).netloc

        pooler_cache_key: str = ""
        if _private_s3_endpoint:
            logger.debug(f"Using private S3 endpoint: {_private_s3_endpoint}")
            _s3_private_domain: str = urlparse(_private_s3_endpoint).netloc
            self._s3_private_domain = _s3_private_domain
            self._s3_endpoint_subject = _S3_PUBLIC_DOMAIN
            pooler_cache_key += _s3_private_domain

        if _timeout_seconds is not None:
            self._timeout_seconds = _timeout_seconds

        self._cache_config = ClientCacheConfig(
            ssl_ca_cert=ssl_ca_cert,
            whylabs_api_endpoint=self.whylabs_api_endpoint,
            endpoint_hostname=self._endpoint_hostname,
            api_key=self._api_key,
        )

        # Just ignore the other args if api_client was passed in. They're saved in the cache config if we need them.
        self._custom_api_client: ApiClient = api_client
        self._api_client: ApiClient = None  # lazily instantiated when needed

        # TODO: if api_client is passed in, this key refresher is only used to print the key id from the
        # env, it isn't actually in the api client because someone else constructed the client and its config.
        self._key_refresher = EnvironmentKeyRefresher() if api_client else None

        # Using a pooler for uploading data
        pool = _UPLOAD_POOLER_CACHE.get(pooler_cache_key)
        if pool is None:
            logger.debug(f"Pooler is not available. Creating a new one for key: {pooler_cache_key}")
            if self._proxy:
                proxy_url = self._proxy
                default_headers = get_auth_headers(proxy_url)
                pool = ProxyManager(
                    proxy_url,
                    num_pools=4,
                    maxsize=10,
                    proxy_headers=default_headers,
                    assert_hostname=self._s3_endpoint_subject,
                    server_hostname=self._s3_endpoint_subject,
                )
            else:
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
        self._refresh_client()
        return self._key_refresher.key_id

    def _refresh_client(self) -> None:
        if self._custom_api_client is None:
            self._api_client, self._key_refresher = WhylabsClientCache.instance().get_client(self._cache_config)

    def _update_hostname_config(self, endpoint_hostname_override: str) -> None:
        """
        This method overrides the pool manager's new connection method to add the hostname

        """
        import urllib3

        self._refresh_client()
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
        prefer_sync: Optional[bool] = None,
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
            raise ValueError("Manual configuration is not supported. Please override the api_client instead")
        if api_client is not None:
            self._custom_api_client = api_client
            self._api_client = None
        if timeout_seconds is not None:
            self._timeout_seconds = timeout_seconds
        if prefer_sync is not None:
            self._prefer_sync = prefer_sync

        self._cache_config = ClientCacheConfig(
            api_key=self._api_key or self._cache_config.api_key,
            ssl_ca_cert=ssl_ca_cert or self._cache_config.ssl_ca_cert,
            whylabs_api_endpoint=self._cache_config.whylabs_api_endpoint,
            endpoint_hostname=self._cache_config.endpoint_hostname,
        )
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

    def tag_custom_performance_column(
        self, column: str, label: Optional[str] = None, default_metric: str = "mean"
    ) -> Tuple[bool, str]:
        """Sets the column as a custom performance metric for the specified dataset and org id.
        The default metric will be displayed in the Performance tab in WhyLabs.

        Parameters
        ----------
        column : str
            The column name in the whylogs profile you want to tag as a custom performance metric.
        label : str
            The label that will be displayed in WhyLabs UI. If none is passed, defaults to column name.
        default_metric : str
            The default metric that will be displayed in the Performance tab in WhyLabs.
            For example, "mean", "median", "max", or "min".
            If none is passed, defaults to "mean".

        Note: the resulting custom performance metric is considered an unmergeable metric.

        """
        self._refresh_client()
        if not label:
            label = column
        api_instance = ModelsApi(self._api_client)
        metric_schema = MetricSchema(
            label=label,
            column=column,
            default_metric=default_metric,
        )
        try:
            res = api_instance.put_entity_schema_metric(self._org_id, self._dataset_id, metric_schema)
            return True, str(res)
        except Exception as e:
            logger.warning(
                f"Failed to tag column {column} as custom performance metric for {self._org_id}/{self._dataset_id} to "
                + f"{self.whylabs_api_endpoint}"
            )
            return False, str(e)

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

    def _write_segmented_result_set(self, file: SegmentedResultSet, **kwargs: Any) -> Tuple[bool, str]:
        """Put segmented result set for the specified dataset.

        Parameters
        ----------
        file : SegmentedResultSet
            SegmentedResultSet object representing the segmented result set for the specified dataset

        Returns
        -------
        Tuple[bool, str]
        """
        # multi-profile writer
        files = file.get_writables()
        messages: List[str] = list()
        and_status: bool = True
        if not files:
            logger.warning("Attempt to write a result set with no writables, nothing written!")
            return True, ""

        logger.debug(f"About to write {len(files)} files:")
        # TODO: special handling of large number of files, handle throttling
        for view in files:
            bool_status, message = self.write(file=view, **kwargs)
            and_status = and_status and bool_status
            messages.append(message)
        logger.debug(f"Completed writing {len(files)} files!")

        return and_status, "; ".join(messages)

    def _tag_custom_perf_metrics(self, view: Union[DatasetProfileView, SegmentedDatasetProfileView]):
        if isinstance(view, DatasetProfileView):
            column_names = view.get_columns().keys()
            for column_name in column_names:
                for perf_col in KNOWN_CUSTOM_PERFORMANCE_METRICS:
                    if column_name.startswith(perf_col):
                        metric = KNOWN_CUSTOM_PERFORMANCE_METRICS[perf_col]
                        self.tag_custom_performance_column(column_name, default_metric=metric)
        return

    @deprecated_alias(profile="file")
    def write(self, file: Writable, **kwargs: Any) -> Tuple[bool, str]:
        if isinstance(file, FeatureWeights):
            return self.write_feature_weights(file, **kwargs)
        elif isinstance(file, EstimationResult):
            return self.write_estimation_result(file, **kwargs)
        elif isinstance(file, ResultSet):
            if isinstance(file, SegmentedResultSet):
                if self._reference_profile_name is not None:
                    return self._write_segmented_reference_result_set(file, **kwargs)
                else:
                    return self._write_segmented_result_set(file, **kwargs)

            file = file.profile()

        view = file.view() if isinstance(file, DatasetProfile) else file

        has_segments = isinstance(view, SegmentedDatasetProfileView)
        has_performance_metrics = view.model_performance_metrics
        self._tag_custom_perf_metrics(view)
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
            if has_performance_metrics or kwargs.get("use_v0"):
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
            if time_delta_seconds > FIVE_YEARS_IN_SECONDS:
                logger.error(
                    f"A profile being uploaded to WhyLabs has a dataset_timestamp of({dataset_timestamp}) "
                    f"compared to current datetime: {utc_now}. Uploads of profiles older than 5 years "
                    "might not be monitored in WhyLabs and may take up to 24 hours to show up."
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

    def _do_get_feature_weights(self):
        """Get latest version for the feature weights for the specified dataset

        Returns
        -------
            Response of the GET request, with segmentWeights and metadata.
        """
        result = self._get_column_weights()
        return result

    def _do_upload_feature_weights(self) -> Tuple[bool, str]:
        """Put feature weights for the specified dataset.

        Returns
        -------
        Tuple[bool, str]
            Tuple with a boolean (1-success, 0-fail) and string with the request's status code.
        """

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

    def _require(self, name: str, value: Optional[str]) -> None:
        if value is None:
            session = _default_init()
            session_type = session.get_type().value
            raise ValueError(
                f"Can't determine {name}. Current session type is {session_type}. "
                f"See {_INIT_DOCS} for instructions on using why.init()."
            )

    def _validate_client(self) -> None:
        self._refresh_client()
        self._require("org id", self._org_id)
        self._require("default dataset id", self._dataset_id)
        self._require("api key", self._cache_config.api_key)

    def _get_or_create_feature_weights_client(self) -> FeatureWeightsApi:
        self._validate_client()
        return FeatureWeightsApi(self._api_client)

    def _get_or_create_models_client(self) -> ModelsApi:
        self._validate_client()
        return ModelsApi(self._api_client)

    def _get_or_create_api_log_client(self) -> LogApi:
        self._validate_client()
        return LogApi(self._api_client)

    def _get_or_create_api_dataset_client(self) -> DatasetProfileApi:
        self._validate_client()
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
        if not alias:
            alias = None
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
                f"Failed to retrieve column schema {self._org_id}/{self._dataset_id} for column name: ({column_name}) "
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
                async_req=not self._prefer_sync,
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
                org_id=self._org_id,
                model_id=self._dataset_id,
                log_reference_request=request,
                async_req=not self._prefer_sync,
            )
            result = async_result if self._prefer_sync else async_result.get()
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
