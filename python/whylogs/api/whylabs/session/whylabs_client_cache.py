import abc
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from urllib3 import ProxyManager
from whylabs_client import ApiClient, Configuration  # type: ignore

from whylogs.core.utils.utils import get_auth_headers


class KeyRefresher(abc.ABC):
    @property
    @abc.abstractmethod
    def key_id(self) -> str:
        pass

    @abc.abstractmethod
    def __call__(self, config: Configuration) -> None:
        pass

    def _validate_api_key(self, api_key: Optional[str]) -> str:
        if api_key is None:
            raise ValueError("Missing API key. Set it via WHYLABS_API_KEY environment variable or as an api_key option")
        if len(api_key) < 12:
            raise ValueError("API key too short")
        if len(api_key) > 80:
            raise ValueError("API key too long")
        if api_key[10] != ".":
            raise ValueError("Invalid format. Expecting a dot at an index 10")
        return api_key[:10]


class StaticKeyRefresher(KeyRefresher):
    def __init__(self, api_key: str) -> None:
        self._key_id = self._validate_api_key(api_key)
        self._api_key = api_key

    @property
    def key_id(self) -> str:
        return self._key_id

    def __call__(self, config: Configuration) -> None:
        config.api_key = {"ApiKeyAuth": self._api_key}

    def __hash__(self) -> int:
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
        from whylogs.api.whylabs.session.session_manager import default_init

        session = default_init()
        session_config = session.config
        api_key = session_config.get_env_api_key()
        self._key_id = self._validate_api_key(api_key)
        assert api_key is not None
        config.api_key = {"ApiKeyAuth": api_key}


@dataclass(frozen=True)
class ClientCacheConfig:
    api_key: Optional[str] = None
    ssl_ca_cert: Optional[str] = None
    whylabs_api_endpoint: Optional[str] = None
    endpoint_hostname: Optional[str] = None  # TODO What is this thing?


class WhylabsClientCache:
    __instance: Optional["WhylabsClientCache"] = None

    # Must be initialized from the session code as a side effect of why.init()
    @staticmethod
    def __init_instance() -> None:
        if WhylabsClientCache.__instance is None:
            WhylabsClientCache.__instance = WhylabsClientCache()

    @staticmethod
    def reset() -> None:
        WhylabsClientCache.__instance = None

    @staticmethod
    def instance() -> "WhylabsClientCache":
        if WhylabsClientCache.__instance is None:
            # Internally, we'll call default_init() to initialize the instance wherever we need
            # a guarantee that one exists.
            raise ValueError("WhylabsClientCache is not initialized. Call why.init() to initialize it.")
        return WhylabsClientCache.__instance

    def __init__(self) -> None:
        self._api_client_cache: Dict[ClientCacheConfig, Tuple[ApiClient, KeyRefresher]] = dict()
        self._logger = logging.getLogger(__name__)

    def get_client(self, config: ClientCacheConfig) -> Tuple[ApiClient, KeyRefresher]:
        entry = self._api_client_cache.get(config)

        if entry is None:
            entry = self._create_client(config)
            self._api_client_cache[config] = entry

        return entry

    def _create_client(self, cache_config: ClientCacheConfig) -> Tuple[ApiClient, KeyRefresher]:
        """
        Refresh the API client by comparing various configs. We try to
        re-use the client as much as we can since using a new client
        every time can be expensive.
        """
        from whylogs.api.whylabs.session.session_manager import default_init

        session = default_init()
        session_config = session.config
        self._proxy = session_config.get_https_proxy() or session_config.get_http_proxy()

        config = Configuration()
        config.api_key = {"ApiKeyAuth": ""}
        refresher: KeyRefresher = (
            StaticKeyRefresher(cache_config.api_key)
            if cache_config.api_key
            else EnvironmentKeyRefresher()  # TODO when would this be used? I'm requiring api keys right now in the whylabs writer
        )
        config.refresh_api_key_hook = refresher
        config.discard_unknown_keys = True
        config.client_side_validation = False  # Disable client side validation and trust the server

        if self._proxy:
            config.proxy = self._proxy
            default_header = get_auth_headers(self._proxy)
            if default_header:
                config.proxy_headers = default_header

        if cache_config.ssl_ca_cert:
            config.ssl_ca_cert = cache_config.ssl_ca_cert

        config.host = cache_config.whylabs_api_endpoint or session_config.get_whylabs_endpoint()

        client = ApiClient(config)
        from whylogs import __version__ as _version

        client.user_agent = f"whylogs/python/{_version}"

        if cache_config.endpoint_hostname:
            self._logger.info(
                f"Override endpoint hostname for TLS verification is set to: {cache_config.endpoint_hostname}"
            )
            self._update_hostname_config(client, cache_config.endpoint_hostname)

        return client, refresher

    def _update_hostname_config(self, client: ApiClient, endpoint_hostname_override: str) -> None:
        """
        This method overrides the pool manager's new connection method to add the hostname
        """
        if isinstance(client.rest_client.pool_manager, ProxyManager):
            raise ValueError("Endpoint hostname override is not supported when using with proxy")

        self._logger.debug(f"Override endpoint hostname to: {endpoint_hostname_override}")
        old_conn_factory = client.rest_client.pool_manager.connection_from_host

        def new_conn_factory(host: str, port: int, scheme: str, pool_kwargs: Optional[Dict[str, str]] = None) -> Any:
            if pool_kwargs is None:
                pool_kwargs = {}
            pool_kwargs["assert_hostname"] = endpoint_hostname_override
            pool_kwargs["server_hostname"] = endpoint_hostname_override
            return old_conn_factory(host, port, scheme, pool_kwargs)

        client.rest_client.pool_manager.connection_from_host = new_conn_factory
