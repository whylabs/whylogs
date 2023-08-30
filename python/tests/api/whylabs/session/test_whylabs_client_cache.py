from whylogs.api.whylabs.session.whylabs_client_cache import ClientCacheConfig


def test_empty_cache_key_works() -> None:
    cache = dict()

    key1 = ClientCacheConfig()
    cache[key1] = 1

    key2 = ClientCacheConfig()
    cache[key2] = 2

    assert len(cache) == 1
    assert cache[key1] == 2


def test_cache_key_works() -> None:
    cache = dict()

    key1 = ClientCacheConfig(
        api_key="api_key", endpoint_hostname="endpoint_hostname", whylabs_api_endpoint="whylabs_api_endpoint"
    )
    cache[key1] = 1

    key2 = ClientCacheConfig(
        api_key="api_key", endpoint_hostname="endpoint_hostname", whylabs_api_endpoint="whylabs_api_endpoint"
    )
    cache[key2] = 2

    key3 = ClientCacheConfig(api_key="api_key")
    cache[key3] = 3

    assert len(cache) == 2
    assert cache[key1] == 2
    assert cache[key2] == 2
    assert cache[key3] == 3
