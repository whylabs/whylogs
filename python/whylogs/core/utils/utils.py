import datetime
import functools
import warnings
from typing import Any, Callable, Dict
from urllib.parse import urlparse

from urllib3 import util


def deprecated(message):
    def decorator_deprecated(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func(*args, **kwargs)
            warnings.warn(message, DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        return wrapper

    return decorator_deprecated


def deprecated_alias(**aliases: str) -> Callable:
    def deprecated_decorator(method: Callable):
        @functools.wraps(method)
        def wrapper(*args, **kwargs):
            rename_kwargs(method.__name__, kwargs, aliases)
            return method(*args, **kwargs)

        return wrapper

    return deprecated_decorator


def rename_kwargs(func_name: str, kwargs: Dict[str, Any], aliases: Dict[str, str]) -> None:
    for alias, new in aliases.items():
        if alias in kwargs:
            if new in kwargs:
                raise TypeError(
                    f"{func_name} received both {alias} and {new} as arguments!"
                    f" {alias} is deprecated, use {new} instead."
                )
            warnings.warn(
                message=f"`{alias}` is deprecated as an argument to `{func_name}`; use" f" `{new}` instead.",
                category=DeprecationWarning,
                stacklevel=3,
            )
            kwargs[new] = kwargs.pop(alias)


def ensure_timezone(dt: datetime.datetime) -> None:
    if dt.tzinfo is None:
        raise ValueError("whylogs requires timezone-aware datetime. Please use datetime.datetime(..., tzinfo=..) API")


def get_auth_headers(proxy_url: str) -> Dict[str, str]:
    parsed_url = urlparse(proxy_url)
    if parsed_url.username and parsed_url.password:
        default_headers = util.make_headers(proxy_basic_auth=f"{str(parsed_url.username)}:{str(parsed_url.password)}")
    else:
        default_headers = None
    return default_headers
