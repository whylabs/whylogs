from typing import Callable, Dict, Generic, Optional, TypeVar

import whylogs_datasketches as ds  # type: ignore

from whylogs_v1.core.metrics.decorators import (
    DecoratedFunction,
    _decorate_func,
    _func_wrapper,
)
from whylogs_v1.core.proto import (
    FrequentItemsSketchMessage,
    HllSketchMessage,
    KllSketchMessage,
    MetricComponentMessage,
)

M = TypeVar("M")
NUM = TypeVar("NUM", float, int)

_MAX_BUILT_IN_ID = 100


class Serializer(DecoratedFunction, Generic[M]):
    def __init__(self, *, func: Callable[[M], MetricComponentMessage], name: str):
        self._name = name
        self._func = func

    @property
    def name(self) -> str:
        return self._name

    def __call__(self, *, value: M) -> MetricComponentMessage:
        return self._func(value)

    @classmethod
    def build(cls, func: "function", name: str) -> "Serializer":  # noqa
        return Serializer(func=func, name=name)  # type: ignore


_TYPED_SERIALIZERS: Dict[type, Serializer] = {}
_INDEXED_SERIALIZERS: Dict[int, Serializer] = {}


def _builtin_serializer(*, name: str) -> Callable[[Callable], Serializer]:
    """Decorator for a builtin field aggregator.

    Note that since these are built in, they MUST be unique and stable over time.
    Args:
        name: a human readable string. This must be unique for validation.

    """

    def decorated(func: Callable) -> Serializer:
        annotations: Dict[str, type] = func.__annotations__.copy()
        if annotations["return"] != MetricComponentMessage:
            raise ValueError("Invalid function type: return type is not MetricComponentMessage")
        annotations.pop("return")
        arg_len = len(annotations.items())
        if arg_len != 1:
            raise ValueError(f"Expected 1 argument, got: {arg_len}")

        agg = Serializer(func=func, name=name)
        input_type = next(iter(annotations.values()))

        _func_wrapper(
            func=func,
            key=input_type,
            name=f"builtin.{input_type}",
            wrapper_dict=_TYPED_SERIALIZERS,
            clazz=Serializer,
        )

        return agg

    return decorated


@_builtin_serializer(name="n")
def _int(value: int) -> MetricComponentMessage:
    return MetricComponentMessage(n=int(value))


@_builtin_serializer(name="d")
def _float(value: float) -> MetricComponentMessage:
    return MetricComponentMessage(d=value)


@_builtin_serializer(name="kll")
def _kll(sketch: ds.kll_doubles_sketch) -> MetricComponentMessage:
    return MetricComponentMessage(kll=KllSketchMessage(sketch=sketch.serialize(), k=sketch.get_k()))


@_builtin_serializer(name="kll")
def _hll_merge(sketch: ds.hll_sketch) -> MetricComponentMessage:
    return MetricComponentMessage(hll=HllSketchMessage(sketch=sketch.serialize_compact(), lg_k=sketch.lg_config_k))


@_builtin_serializer(name="kll")
def _fs_merge(sketch: ds.frequent_strings_sketch) -> MetricComponentMessage:
    msg = FrequentItemsSketchMessage(sketch=sketch.serialize(), lg_max_k=12)

    return MetricComponentMessage(frequent_items=msg)


class SerializerRegistry:
    def __init__(self) -> None:
        self._typed_serializers = _TYPED_SERIALIZERS.copy()
        self._indexed_serializer = _INDEXED_SERIALIZERS.copy()

    def get(self, *, mtype: Optional[type] = None, index: int = 0) -> Optional[Serializer]:
        if mtype is None and index <= 0:
            raise ValueError("Please specify mtype or index")
        res: Optional[Serializer] = None
        if index > 0:
            res = self._indexed_serializer.get(index)

        if res is None and mtype is not None:
            res = self._typed_serializers.get(mtype)

        return res


_STANDARD_REGISTRY = SerializerRegistry()


def get_serializer(
    *, mtype: Optional[type] = None, index: int = 0, registry: Optional[SerializerRegistry] = None
) -> Optional[Serializer]:
    if registry is None:
        registry = _STANDARD_REGISTRY
    return registry.get(mtype=mtype, index=index)


def serializer(*, index: int, registry: SerializerRegistry = _STANDARD_REGISTRY):  # type: ignore
    if index < _MAX_BUILT_IN_ID:
        raise ValueError("Custom aggregator identifier must be equal or greater than 100")

    if registry is None:
        registry = _STANDARD_REGISTRY

    return _decorate_func(
        key=index, name=f"custom.{index}", wrapper_dict=registry._indexed_serializer, clazz=Serializer
    )
