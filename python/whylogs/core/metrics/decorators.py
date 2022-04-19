from abc import ABC, abstractmethod
from types import FunctionType, LambdaType, MethodType
from typing import Any, Dict, Type, TypeVar, Union

DF = TypeVar("DF", bound="DecoratedFunction")

FuncType = Union[FunctionType, MethodType, LambdaType]


class DecoratedFunction(ABC):
    _func: FuncType  # noqa
    _name: str

    @classmethod
    @abstractmethod
    def build(cls: Type[DF], func: FuncType, name: str) -> DF:  # noqa
        pass


def _decorate_func(  # type: ignore
    *,
    key: Any,  # type: ignore
    name: str,  # type: ignore
    wrapper_dict: Dict[Any, DF],
    clazz: Type[DF],
):
    def decorated(func: FunctionType) -> DF:  # noqa
        return _func_wrapper(func=func, key=key, name=name, wrapper_dict=wrapper_dict, clazz=clazz)

    return decorated


def _func_wrapper(
    *,
    func: FuncType,
    key: Any,
    name: str,
    wrapper_dict: Dict[Any, DF],
    clazz: Type[DF],
) -> DF:
    existing_ = wrapper_dict.get(key)
    if existing_ is not None:
        # same annotation could be executed twice an import so we
        # want to detect duplication here. Probably buggy because we rely on
        # the code and name to detect duplicates? Good enough?
        if (
            existing_._func.__name__ == func.__name__
            and existing_._func.__code__.__hash__() == func.__code__.__hash__()  # type: ignore
        ):
            return existing_
        raise ValueError(f"Builtin aggregator for type {key} is already registered")

    wrapped_func = clazz.build(func=func, name=name)
    wrapper_dict[key] = wrapped_func
    return wrapped_func
