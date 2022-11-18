import functools
import warnings
from typing import Any, Callable, Dict


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


def rename_kwargs(func_name: str, kwargs: Dict[str, Any], aliases: Dict[str, str]):
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
