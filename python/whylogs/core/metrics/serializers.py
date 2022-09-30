from typing import Any, Callable, Dict, Generic, Optional, TypeVar

import whylogs_sketching as ds  # type: ignore

from whylogs.core.metrics.decorators import (
    DecoratedFunction,
    FuncType,
    _decorate_func,
    _func_wrapper,
)
from whylogs.core.proto import (
    FrequentItemsSketchMessage,
    HllSketchMessage,
    KllSketchMessage,
    MetricComponentMessage,
)

M = TypeVar("M")
NUM = TypeVar("NUM", float, int)

_MAX_BUILT_IN_ID = 100


class Serializer(DecoratedFunction, Generic[M]):
    def __init__(self, *, func: FuncType, name: str):
        self._name = name
        self._func = func

    @property
    def name(self) -> str:
        return self._name

    def __call__(self, *, value: M) -> MetricComponentMessage:
        return self._func(value)

    @classmethod
    def build(cls, func: FuncType, name: str) -> "Serializer":  # noqa
        return Serializer(func=func, name=name)  # type: ignore


_TYPED_SERIALIZERS: Dict[type, Serializer] = {}
_INDEXED_SERIALIZERS: Dict[int, Serializer] = {}


def _builtin_serializer(*, name: str) -> Callable[[Callable], Serializer]:
    """Decorator for a builtin field aggregator.

    Note that since these are built in, they MUST be unique and stable over time.
    Args:
        name: a human readable string. This must be unique for validation.

    """

    def decorated(func: FuncType) -> Serializer:
        annotations: Dict[str, type] = func.__annotations__.copy()
        if annotations["return"] != MetricComponentMessage:
            raise ValueError("Invalid function type: return type is not MetricComponentMessage")
        annotations.pop("return")
        arg_len = len(annotations.items())
        if arg_len != 1:
            raise ValueError(f"Expected 1 argument, got: {arg_len}")

        ser = Serializer[Any](func=func, name=name)
        input_type = next(iter(annotations.values()))

        _func_wrapper(
            func=func,
            key=input_type,
            name=f"builtin.{input_type}",
            wrapper_dict=_TYPED_SERIALIZERS,
            clazz=Serializer,
        )

        return ser

    return decorated  # type: ignore


@_builtin_serializer(name="n")
def _int(value: int) -> MetricComponentMessage:
    return MetricComponentMessage(n=int(value))


@_builtin_serializer(name="d")
def _float(value: float) -> MetricComponentMessage:
    return MetricComponentMessage(d=value)


@_builtin_serializer(name="kll")
def _kll(sketch: ds.kll_doubles_sketch) -> MetricComponentMessage:
    return MetricComponentMessage(kll=KllSketchMessage(sketch=sketch.serialize()))


@_builtin_serializer(name="kll")
def _hll_merge(sketch: ds.hll_sketch) -> MetricComponentMessage:
    return MetricComponentMessage(hll=HllSketchMessage(sketch=sketch.serialize_compact()))


@_builtin_serializer(name="kll")
def _fs_merge(sketch: ds.frequent_strings_sketch) -> MetricComponentMessage:
    msg = FrequentItemsSketchMessage(sketch=sketch.serialize())

    return MetricComponentMessage(frequent_items=msg)


class SerializerRegistry:
    def __init__(self) -> None:
        self._typed_serializers = _TYPED_SERIALIZERS.copy()
        self._id_serializer = _INDEXED_SERIALIZERS.copy()

    def get(self, *, mtype: Optional[type] = None, type_id: int = 0) -> Optional[Serializer]:
        if mtype is None and type_id <= 0:
            raise ValueError("Please specify mtype or id")
        res: Optional[Serializer] = None
        if type_id > 0:
            res = self._id_serializer.get(type_id)

        if res is None and mtype is not None:
            res = self._typed_serializers.get(mtype)

        return res


_STANDARD_REGISTRY = None


def _get_or_create_registry() -> SerializerRegistry:
    global _STANDARD_REGISTRY
    if _STANDARD_REGISTRY is None:
        _STANDARD_REGISTRY = SerializerRegistry()

    return _STANDARD_REGISTRY


def get_serializer(
    *, mtype: Optional[type] = None, type_id: int = 0, registry: Optional[SerializerRegistry] = None
) -> Optional[Serializer]:
    if registry is None:
        registry = _get_or_create_registry()
    return registry.get(mtype=mtype, type_id=type_id)


def serializer(*, type_id: int, registry: Optional[SerializerRegistry] = None):  # type: ignore
    if type_id < _MAX_BUILT_IN_ID:
        raise ValueError("Custom aggregator id must be equal or greater than 100")

    if registry is None:
        registry = _get_or_create_registry()

    return _decorate_func(key=type_id, name=f"custom.{type_id}", wrapper_dict=registry._id_serializer, clazz=Serializer)
