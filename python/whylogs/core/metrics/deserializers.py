from typing import Any, Callable, Dict, Optional, TypeVar

import whylogs_sketching as ds  # type: ignore

from whylogs.core.metrics.decorators import (
    DecoratedFunction,
    FuncType,
    _decorate_func,
    _func_wrapper,
)
from whylogs.core.proto import MetricComponentMessage

M = TypeVar("M")
NUM = TypeVar("NUM", float, int)

_MAX_BUILT_IN_ID = 100


class _Deserializer(DecoratedFunction):
    def __init__(self, *, func: FuncType, name: str):  # noqa
        self._name = name
        self._func = func

    @property
    def name(self) -> Optional[str]:
        return self._name

    def __call__(self, *, msg: MetricComponentMessage) -> Any:
        return self._func(msg)

    @classmethod
    def build(cls, func: FuncType, name: str) -> "_Deserializer":  # noqa
        return _Deserializer(func=func, name=name)


# these are all built in and initialized at imports
_TYPED_DESERIALIZERS: Dict[type, _Deserializer] = {}
_ID_DESERIALIZERS: Dict[int, _Deserializer] = {}


def _builtin_deserializer(*, name: str) -> Callable[[Callable], _Deserializer]:
    """Decorator for a builtin field aggregator.

    Note that since these are built in, they MUST be unique and stable over time.
    Args:
        name: a human readable string. This must be unique for validation.

    """

    def decorated(func: FuncType) -> _Deserializer:
        annotations: Dict[str, type] = func.__annotations__.copy()
        r_type = annotations.pop("return")

        arg_len = len(annotations.items())
        if arg_len != 1:
            raise ValueError(f"Expected 1 argument, got: {arg_len}")

        if next(iter(annotations.values())) != MetricComponentMessage:
            raise ValueError("Invalid function type: input argument not MetricComponentMessage")

        return _func_wrapper(
            func=func, key=r_type, name=f"builtin.{r_type}", wrapper_dict=_TYPED_DESERIALIZERS, clazz=_Deserializer
        )

    return decorated  # type: ignore


@_builtin_deserializer(name="int")
def _int(msg: MetricComponentMessage) -> int:
    field = msg.WhichOneof("value")
    if field != "n":
        raise ValueError(f"Unsupported field: {field}")
    return msg.n


@_builtin_deserializer(name="float")
def _float(msg: MetricComponentMessage) -> float:
    field = msg.WhichOneof("value")
    if field != "d":
        raise ValueError(f"Unsupported field: {field}")
    return msg.d


@_builtin_deserializer(name="kll")
def _kll(msg: MetricComponentMessage) -> ds.kll_doubles_sketch:
    field = msg.WhichOneof("value")
    if field != "kll":
        raise ValueError(f"Unsupported field: {field}")

    return ds.kll_doubles_sketch.deserialize(msg.kll.sketch)


@_builtin_deserializer(name="kll")
def _hll_merge(msg: MetricComponentMessage) -> ds.hll_sketch:
    field = msg.WhichOneof("value")
    if field != "hll":
        raise ValueError(f"Unsupported field: {field}")

    sketch = ds.hll_sketch.deserialize(msg.hll.sketch)
    return sketch


@_builtin_deserializer(name="kll")
def _fs_merge(msg: MetricComponentMessage) -> ds.frequent_strings_sketch:
    field = msg.WhichOneof("value")
    if field != "frequent_items":
        raise ValueError(f"Unsupported field: {field}")

    return ds.frequent_strings_sketch.deserialize(msg.frequent_items.sketch)


class DeserializerRegistry:
    def __init__(self) -> None:
        self._typed_deserializer = _TYPED_DESERIALIZERS.copy()
        self._id_deserializer = _ID_DESERIALIZERS.copy()

    def get(self, *, mtype: Optional[type] = None, type_id: int = 0) -> Optional[_Deserializer]:
        if mtype is None and type_id <= 0:
            raise ValueError("Either mtype or a positive ID must be specified")
        result = None
        if type_id > 0:
            result = self._id_deserializer.get(type_id)

        if result is None and mtype is not None:
            result = self._typed_deserializer.get(mtype)
        return result


_STANDARD_REGISTRY = None


def _get_or_create_registry() -> DeserializerRegistry:
    global _STANDARD_REGISTRY
    if _STANDARD_REGISTRY is None:
        _STANDARD_REGISTRY = DeserializerRegistry()

    return _STANDARD_REGISTRY


def get_deserializer(
    *, mtype: Optional[type] = None, type_id: int = 0, registry: Optional[DeserializerRegistry] = None
) -> Optional[_Deserializer]:
    if registry is None:
        registry = _get_or_create_registry()
    return registry.get(mtype=mtype, type_id=type_id)


def deserializer(*, type_id: int, registry: Optional[DeserializerRegistry] = None):  # type: ignore
    if type_id < _MAX_BUILT_IN_ID:
        raise ValueError("Custom aggregator ID must be equal or greater than 100")
    if registry is None:
        registry = _get_or_create_registry()

    return _decorate_func(
        key=type_id, name=f"custom.{type_id}", wrapper_dict=registry._id_deserializer, clazz=_Deserializer
    )
