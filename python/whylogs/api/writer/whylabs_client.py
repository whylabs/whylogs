import logging
import os
import pprint
from typing import IO, Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import backoff
import requests  # type: ignore
from urllib3 import PoolManager, ProxyManager
from whylabs_client import ApiClient, Configuration  # type: ignore
from whylabs_client.api.dataset_profile_api import DatasetProfileApi  # type: ignore
from whylabs_client.api.feature_weights_api import FeatureWeightsApi  # type: ignore
from whylabs_client.api.log_api import (
    LogApi,
    LogAsyncRequest,
    LogReferenceRequest,
    LogReferenceResponse,
)
from whylabs_client.api.models_api import ModelsApi  # type: ignore
from whylabs_client.api.transactions_api import TransactionsApi
from whylabs_client.exceptions import NotFoundException
from whylabs_client.model.async_log_response import AsyncLogResponse
from whylabs_client.model.column_schema import ColumnSchema  # type: ignore
from whylabs_client.model.create_reference_profile_request import (  # type: ignore
    CreateReferenceProfileRequest,
)
from whylabs_client.model.log_transaction_metadata import LogTransactionMetadata
from whylabs_client.model.metric_schema import MetricSchema  # type: ignore
from whylabs_client.model.segment import Segment  # type: ignore
from whylabs_client.model.segment_tag import SegmentTag  # type: ignore
from whylabs_client.model.transaction_commit_request import TransactionCommitRequest
from whylabs_client.model.transaction_log_request import TransactionLogRequest
from whylabs_client.model.transaction_start_request import TransactionStartRequest
from whylabs_client.rest import (  # type: ignore
    ApiException,
    ForbiddenException,
    ServiceException,
)

from whylogs.api.logger.result_set import ResultSet, SegmentedResultSet
from whylogs.api.whylabs.session.session_manager import INIT_DOCS, default_init
from whylogs.api.whylabs.session.whylabs_client_cache import (
    ClientCacheConfig,
    EnvironmentKeyRefresher,
    KeyRefresher,
    WhylabsClientCache,
)
from whylogs.context.environ import read_bool_env_var
from whylogs.core import DatasetProfile, DatasetProfileView
from whylogs.core.feature_weights import FeatureWeights
from whylogs.core.utils.utils import get_auth_headers
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView

FIVE_MINUTES_IN_SECONDS = 60 * 5
DAY_IN_SECONDS = 60 * 60 * 24
FIVE_YEARS_IN_SECONDS = DAY_IN_SECONDS * 365 * 5
logger = logging.getLogger(__name__)
logging.getLogger("backoff").addHandler(logging.StreamHandler())

WHYLOGS_PREFER_SYNC_KEY = "WHYLOGS_PREFER_SYNC"

_API_CLIENT_CACHE: Dict[str, ApiClient] = dict()
_UPLOAD_POOLER_CACHE: Dict[str, Union[PoolManager, ProxyManager]] = dict()

_US_WEST2_DOMAIN = "songbird-20201223060057342600000001.s3.us-west-2.amazonaws.com"
_S3_PUBLIC_DOMAIN = os.environ.get("_WHYLABS_PRIVATE_S3_DOMAIN") or _US_WEST2_DOMAIN


MAX_REQUEST_TIME = 60  # seconds
MAX_REQUEST_TRIES = 10

KNOWN_CUSTOM_PERFORMANCE_METRICS = {
    "mean_average_precision_k_": "mean",
    "accuracy_k_": "mean",
    "reciprocal_rank": "mean",
    "precision_k_": "mean",
    "recall_k_": "mean",
    "top_rank": "mean",
    "average_precision_k_": "mean",
    "norm_dis_cumul_gain_k_": "mean",
    "sum_gain_k_": "mean",
}

KNOWN_CUSTOM_OUTPUT_METRICS = {
    "mean_average_precision_k_": ("fractional", "continuous"),
    "accuracy_k_": ("fractional", "continuous"),
    "reciprocal_rank": ("fractional", "continuous"),
    "precision_k_": ("fractional", "continuous"),
    "recall_k_": ("fractional", "continuous"),
    "top_rank": ("integral", "continuous"),
    "average_precision_k_": ("fractional", "continuous"),
    "norm_dis_cumul_gain_k_": ("fractional", "continuous"),
    "sum_gain_k_": ("fractional", "continuous"),
}


_RETRY_EXCEPTIONS = (ApiException, ServiceException)
_RETRY_CODES = {408, 429, 502, 503, 504}


def _giveup(e) -> bool:
    return (e.status not in _RETRY_CODES,)  # type: ignore


def _get_column_names(x: Union[DatasetProfile, DatasetProfileView, SegmentedDatasetProfileView, ResultSet]) -> Set[str]:
    if isinstance(x, DatasetProfile):
        return _get_column_names(x.view())
    elif isinstance(x, DatasetProfileView):
        return set(x.get_columns().keys())
    elif isinstance(x, SegmentedDatasetProfileView):
        return _get_column_names(x._profile_view)
    elif isinstance(x, SegmentedResultSet):
        maps = x._segments.values()
        if not maps:
            return set()
        segments = list(maps)[0].values()
        if not segments:
            return set()
        segment = list(segments)[0]
        view = segment if isinstance(segment, DatasetProfileView) else segment.view()
        return _get_column_names(view)

    assert isinstance(x, ResultSet)
    return _get_column_names(x.view())


class TransactionAbortedException(Exception):
    pass


class WhyLabsClient:
    f"""
    An interface for interacting with the WhyLabs platform.

    >**IMPORTANT**: In order to correctly interact with WhyLabs, make sure you have
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

    ```python
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
        session = default_init()  # Force an init if the user didn't do it, it's idempotent
        config = session.config

        self._org_id = org_id or config.get_org_id()
        self._dataset_id = dataset_id or config.get_default_dataset_id()
        self._api_key = api_key or config.get_api_key()
        self._ssl_ca_cert = ssl_ca_cert
        self._feature_weights = None
        self._reference_profile_name = config.get_whylabs_refernce_profile_name()
        self._api_config: Optional[Configuration] = None
        self._prefer_sync = read_bool_env_var(WHYLOGS_PREFER_SYNC_KEY, False)
        self._transaction_id: Optional[str] = None

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

    def option(self, **kwargs) -> "WhyLabsClient":  # type: ignore
        """

        Parameters
        ----------
        org_id the organization ID
        dataset_id the dataset Id
        api_key the API key
        reference_profile_name the name of the reference profile
        configuration the additional configuration for the REST client
        transaction_id to ID of currently active transaction

        org_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        api_key: Optional[str] = None,
        reference_profile_name: Optional[str] = None,
        configuration: Optional[Configuration] = None,
        transaction_id: Optional[str] = None,
        ssl_ca_cert: Optional[str] = None,
        api_client: Optional[ApiClient] = None,
        timeout_seconds: Optional[float] = None,
        transaction_id: Optional[str] = None,
        prefer_sync:

        Returns a "WhyLabsClient" with these options configured
        -------

        """
        org_id = kwargs.get("org_id")
        dataset_id = kwargs.get("dataset_id")
        api_key = kwargs.get("api_key")
        reference_profile_name = kwargs.get("reference_profile_name")
        configuration = kwargs.get("configuration")
        ssl_ca_cert = kwargs.get("ssl_ca_cert")
        api_client = kwargs.get("api_client")
        timeout_seconds = kwargs.get("timeout_seconds")
        prefer_sync = kwargs.get("prefer_sync")
        transaction_id = kwargs.get("transaction_id")
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
        if transaction_id is not None:
            self._transaction_id = transaction_id

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
            builtin_metric=None,
        )

        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            return api_instance.put_entity_schema_metric(self._org_id, self._dataset_id, metric_schema)

        try:
            res = do_request()
            return True, str(res)
        except Exception as e:
            logger.warning(
                f"Failed to tag column {column} as custom performance metric for {self._org_id}/{self._dataset_id} to "
                + f"{self.whylabs_api_endpoint}"
            )
            return False, str(e)

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
        if kwargs.get("dataset_id") is not None:
            self._dataset_id = kwargs.get("dataset_id")
        return self._do_upload_feature_weights(file)

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

    def _set_column_schema(self, column_name: str, column_schema: ColumnSchema):
        model_api_instance = self._get_or_create_models_client()

        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            model_api_instance.put_entity_schema_column(  # type: ignore
                self._org_id, self._dataset_id, column_name, column_schema=column_schema
            )

        try:
            # TODO: remove when whylabs supports merge writes.
            do_request()
            return (
                200,
                f"{column_name} schema set to {column_schema.classifier} {column_schema.data_type} {column_schema.discreteness}",
            )
        except ForbiddenException as e:
            logger.exception(
                f"Failed to set column outputs {self._org_id}/{self._dataset_id} for column name: ("
                f"{column_name}) "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {self.key_id}"
            )
            raise e

    def _tag_custom_output_metrics(
        self, view: Union[DatasetProfile, DatasetProfileView, SegmentedDatasetProfileView, ResultSet]
    ) -> None:
        column_names = _get_column_names(view)
        for column_name in column_names:
            for perf_col in KNOWN_CUSTOM_OUTPUT_METRICS:
                if column_name.startswith(perf_col):
                    data_type = KNOWN_CUSTOM_OUTPUT_METRICS[perf_col][0]
                    discreteness = KNOWN_CUSTOM_OUTPUT_METRICS[perf_col][1]
                    column_schema: ColumnSchema = ColumnSchema(
                        classifier="output", data_type=data_type, discreteness=discreteness  # type: ignore
                    )
                    self._set_column_schema(column_name, column_schema=column_schema)

    def _tag_custom_perf_metrics(
        self, view: Union[DatasetProfile, DatasetProfileView, SegmentedDatasetProfileView, ResultSet]
    ) -> None:
        column_names = _get_column_names(view)
        for column_name in column_names:
            for perf_col in KNOWN_CUSTOM_PERFORMANCE_METRICS:
                if column_name.startswith(perf_col):
                    metric = KNOWN_CUSTOM_PERFORMANCE_METRICS[perf_col]
                    self.tag_custom_performance_column(column_name, default_metric=metric)

    def _do_get_feature_weights(self):
        """Get latest version for the feature weights for the specified dataset

        Returns
        -------
            Response of the GET request, with segmentWeights and metadata.
        """
        result = self._get_column_weights()
        return result

    def _do_upload_feature_weights(self, file: FeatureWeights) -> Tuple[bool, str]:
        """Put feature weights for the specified dataset.

        Returns
        -------
        Tuple[bool, str]
            Tuple with a boolean (1-success, 0-fail) and string with the request's status code.
        """

        result = self._put_feature_weights(file)
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

        data = profile_file.read()

        @backoff.on_predicate(
            backoff.expo,
            lambda x: x.status != 200,  # maybe should be == 429
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            return self._s3_pool.request("PUT", upload_url, headers=headers, timeout=self._timeout_seconds, body=data)

        response = do_request()
        is_successful = False
        if response.status == 200:
            is_successful = True
            logger.info(
                f"Done uploading {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                f"{self.whylabs_api_endpoint} with API token ID: {self._key_refresher.key_id}"
            )
        else:
            logger.warning(
                f"response from file upload was not 200, instead got: {pprint.pformat(vars(response), indent=4)}"
            )

        return is_successful, response.reason

    def do_upload(
        self,
        dataset_timestamp: int,
        upload_url: str,
        profile_id: str,
        profile_path: Optional[str] = None,
        profile_file: Optional[IO[bytes]] = None,
    ) -> Tuple[bool, str]:
        assert profile_path or profile_file, "Either a file or file path must be specified when uploading profiles"
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
        return False, "Imposible control flow"

    def _require(self, name: str, value: Optional[str]) -> None:
        if value is None:
            session = default_init()
            session_type = session.get_type().value
            raise ValueError(
                f"Can't determine {name}. Current session type is {session_type}. "
                f"See {INIT_DOCS} for instructions on using why.init()."
            )

    def _validate_client(self) -> None:
        self._refresh_client()
        self._require("org id", self._org_id)
        self._require("default dataset id", self._dataset_id)
        self._require("api key", self._cache_config.api_key)

    def _get_or_create_transaction_client(self) -> TransactionsApi:
        self._refresh_client()
        assert self._api_client is not None
        return TransactionsApi(self._api_client)

    def get_transaction_id(self) -> str:
        client: TransactionsApi = self._get_or_create_transaction_client()
        request = TransactionStartRequest(dataset_id=self._dataset_id)

        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            return client.start_transaction(request)

        result: LogTransactionMetadata = do_request()
        logger.info(f"Starting transaction {result['transaction_id']}, expires {result['expiration_time']}")
        return result["transaction_id"]

    def commit_transaction(self, id: str) -> None:
        logger.info(f"Committing transaction {id}")
        client = self._get_or_create_transaction_client()
        request = TransactionCommitRequest(verbose=True)

        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            client.commit_transaction(id, request)

        # We abandon the transaction if this throws
        try:
            do_request()
        except NotFoundException as e:
            if "Transaction has been aborted" in str(e):  # TODO: perhaps not the most robust test?
                logger.error(f"Transaction {id} was aborted; not committing")
                raise TransactionAbortedException(f"Transaction {id} has been aborted")
            else:
                raise e

    def abort_transaction(self, id: str) -> None:
        logger.info(f"Aborting transaciton {id}")
        client = self._get_or_create_transaction_client()

        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            client.abort_transaction(id)

        do_request()

    def transaction_status(self, id: str) -> Dict[str, Any]:
        client = self._get_or_create_transaction_client()

        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            return client.transaction_status(id)

        return do_request()

    def get_upload_url_transaction(
        self, dataset_timestamp: int, whylabs_tags: List[SegmentTag] = []
    ) -> Tuple[str, str]:
        region = os.getenv("WHYLABS_UPLOAD_REGION", None)
        client: TransactionsApi = self._get_or_create_transaction_client()
        client.api_client.set_default_header("X-WhyLabs-File-Extension", "BIN")
        request = TransactionLogRequest(dataset_timestamp=dataset_timestamp, segment_tags=whylabs_tags, region=region)

        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            return client.log_transaction(self._transaction_id, request)

        result: AsyncLogResponse = do_request()
        return result.id, result.upload_url

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
    def _build_log_async_request(dataset_timestamp: int, region: Optional[str] = None) -> LogAsyncRequest:
        return LogAsyncRequest(dataset_timestamp=dataset_timestamp, segment_tags=[], region=region)

    @staticmethod
    def _build_log_reference_request(
        dataset_timestamp: int, alias: Optional[str] = None, region: Optional[str] = None
    ) -> LogReferenceRequest:
        return LogReferenceRequest(dataset_timestamp=dataset_timestamp, alias=alias, region=region)

    @staticmethod
    def _build_log_segmented_reference_request(
        dataset_timestamp: int, tags: Optional[dict] = None, alias: Optional[str] = None, region: Optional[str] = None
    ) -> LogReferenceRequest:
        segments = list()
        if not alias:
            alias = None
        if tags is not None:
            for segment_tags in tags:
                segments.append(Segment(tags=[SegmentTag(key=tag["key"], value=tag["value"]) for tag in segment_tags]))
        if not segments:
            return CreateReferenceProfileRequest(alias=alias, dataset_timestamp=dataset_timestamp, region=region)
        else:
            return CreateReferenceProfileRequest(
                alias=alias, dataset_timestamp=dataset_timestamp, segments=segments, region=region
            )

    def _get_column_weights(self):
        feature_weight_api = self._get_or_create_feature_weights_client()

        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            return feature_weight_api.get_column_weights(
                org_id=self._org_id,
                dataset_id=self._dataset_id,
            )

        try:
            result = do_request()
            return result
        except ForbiddenException as e:
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {self.key_id}"
            )
            raise e

    def _put_feature_weights(self, file: FeatureWeights):
        feature_weight_api = self._get_or_create_feature_weights_client()

        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            return feature_weight_api.put_column_weights(
                org_id=self._org_id,
                dataset_id=self._dataset_id,
                body={
                    "segmentWeights": [file.to_dict()],
                },
                _return_http_data_only=False,
            )

        try:
            result = do_request()
            return result[1]
        except ForbiddenException as e:
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {self.key_id}"
            )
            raise e

    def _get_existing_column_schema(self, model_api_instance, column_name) -> Optional[ColumnSchema]:
        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            return model_api_instance.get_entity_schema_column(
                org_id=self._org_id, dataset_id=self._dataset_id, column_id=column_name
            )

        try:
            # TODO: remove when whylabs supports merge writes.
            existing_schema = do_request()
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

            @backoff.on_exception(
                backoff.expo,
                _RETRY_EXCEPTIONS,
                giveup=_giveup,
                max_time=MAX_REQUEST_TIME,
                max_tries=MAX_REQUEST_TRIES,
                jitter=backoff.full_jitter,
            )
            def do_request():
                # TODO: remove when whylabs supports merge writes.
                model_api_instance.put_entity_schema_column(
                    self._org_id, self._dataset_id, column_name, updated_column_schema
                )

            try:
                do_request()
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

    def _post_log_async(
        self, request: LogAsyncRequest, dataset_timestamp: int, zip_file: bool = False
    ) -> AsyncLogResponse:
        log_api = self._get_or_create_api_log_client()
        if zip_file:
            log_api.api_client.set_default_header("X-WhyLabs-File-Extension", "ZIP")
        else:
            log_api.api_client.set_default_header("X-WhyLabs-File-Extension", "BIN")

        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            return log_api.log_async(org_id=self._org_id, dataset_id=self._dataset_id, log_async_request=request)

        try:
            result = do_request()
            return result
        except ForbiddenException as e:
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {self.key_id}"
            )
            raise e

    def _get_upload_urls_segmented_reference(
        self, whylabs_tags, dataset_timestamp: int, reference_profile_name: str
    ) -> Tuple[str, List[str]]:
        region = os.getenv("WHYLABS_UPLOAD_REGION", None)
        request = self._build_log_segmented_reference_request(
            dataset_timestamp, tags=whylabs_tags, alias=reference_profile_name, region=region
        )
        res = self._post_log_segmented_reference(
            request=request,
            dataset_timestamp=dataset_timestamp,
        )
        return res["id"], res["upload_urls"]

    def _get_upload_url_segmented_reference_zip(
        self, whylabs_tags, dataset_timestamp: int, reference_profile_name: str
    ) -> Tuple[str, str]:
        region = os.getenv("WHYLABS_UPLOAD_REGION", None)
        request = self._build_log_segmented_reference_request(
            dataset_timestamp, tags=whylabs_tags, alias=reference_profile_name, region=region
        )
        res = self._post_log_segmented_reference(request=request, dataset_timestamp=dataset_timestamp, zip_file=True)
        return res["id"], res["upload_urls"][0]

    def _post_log_segmented_reference(
        self, request: LogAsyncRequest, dataset_timestamp: int, zip_file: bool = False
    ) -> LogReferenceResponse:
        dataset_api = self._get_or_create_api_dataset_client()
        if zip_file:
            dataset_api.api_client.set_default_header("X-WhyLabs-File-Extension", "ZIP")
        else:
            dataset_api.api_client.set_default_header("X-WhyLabs-File-Extension", "BIN")

        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            return dataset_api.create_reference_profile(
                org_id=self._org_id,
                dataset_id=self._dataset_id,
                create_reference_profile_request=request,
                async_req=not self._prefer_sync,
            )

        try:
            async_result = do_request()
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

        @backoff.on_exception(
            backoff.expo,
            _RETRY_EXCEPTIONS,
            giveup=_giveup,
            max_time=MAX_REQUEST_TIME,
            max_tries=MAX_REQUEST_TRIES,
            jitter=backoff.full_jitter,
        )
        def do_request():
            return log_api.log_reference(
                org_id=self._org_id,
                model_id=self._dataset_id,
                log_reference_request=request,
                async_req=not self._prefer_sync,
            )

        try:
            async_result = do_request()
            result = async_result if self._prefer_sync else async_result.get()
            return result
        except ForbiddenException as e:
            logger.exception(
                f"Failed to upload {self._org_id}/{self._dataset_id}/{dataset_timestamp} to "
                f"{self.whylabs_api_endpoint}"
                f" with API token ID: {self.key_id}"
            )
            raise e

    def get_upload_url_batch_zip(self, dataset_timestamp: int) -> Tuple[str, str]:
        region = os.getenv("WHYLABS_UPLOAD_REGION", None)
        request = self._build_log_async_request(dataset_timestamp, region=region)
        res = self._post_log_async(request=request, dataset_timestamp=dataset_timestamp, zip_file=True)
        upload_url = self._update_domain(res["upload_url"])
        profile_id = res["id"]
        return profile_id, upload_url

    def _update_domain(self, upload_url: str) -> str:
        if self._s3_private_domain:
            if _S3_PUBLIC_DOMAIN not in upload_url:
                raise ValueError(
                    "S3 private domain is enabled but your account is not using S3 upload endpoint. " "Aborting!"
                )
            upload_url = upload_url.replace(_S3_PUBLIC_DOMAIN, self._s3_private_domain)
            logger.debug(f"Replaced URL with our private domain. New URL: {upload_url}")
        return upload_url

    def get_upload_url_unsegmented_reference(
        self, dataset_timestamp: int, reference_profile_name: str
    ) -> Tuple[str, str]:
        region = os.getenv("WHYLABS_UPLOAD_REGION", None)
        request = self._build_log_reference_request(dataset_timestamp, alias=reference_profile_name, region=region)
        res = self._post_log_reference(request=request, dataset_timestamp=dataset_timestamp)
        upload_url = self._update_domain(res["upload_url"])
        profile_id = res["id"]
        return profile_id, upload_url

    def get_upload_url_batch(self, dataset_timestamp: int) -> Tuple[str, str]:
        region = os.getenv("WHYLABS_UPLOAD_REGION", None)
        request = self._build_log_async_request(dataset_timestamp, region=region)
        res = self._post_log_async(request=request, dataset_timestamp=dataset_timestamp)
        upload_url = self._update_domain(res["upload_url"])
        profile_id = res["id"]
        return profile_id, upload_url
