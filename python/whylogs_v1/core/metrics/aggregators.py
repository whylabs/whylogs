from typing import Callable, Dict, Generic, Optional, TypeVar

import whylogs_datasketches as ds  # type: ignore

from whylogs_v1.core.metrics.decorators import DecoratedFunction, _decorate_func

M = TypeVar("M")

_MAX_BUILT_IN_INDEX = 100


class _Aggregator(DecoratedFunction, Generic[M]):
    def __init__(self, *, func: "function", name: str):  # noqa
        self._func = func
        self._name = name

    @property
    def name(self) -> Optional[str]:
        return self._name

    def __call__(self, lhs: M, rhs: M) -> M:
        return self._func(lhs, rhs)

    @classmethod
    def build(cls, func: "function", name: str) -> "_Aggregator":  # noqa
        return _Aggregator(func=func, name=name)


_TYPED_AGGREGATORS: Dict[type, _Aggregator] = {}
_INDEXED_AGGREGATORS: Dict[int, _Aggregator] = {}


def _typed_aggregator(*, mtype: type, name: str) -> Callable[[Callable], _Aggregator]:
    decorated = _decorate_func(key=mtype, name=f"builtin.{name}", wrapper_dict=_TYPED_AGGREGATORS, clazz=_Aggregator)

    return decorated


def _indexed_aggregator(*, index: int, name: str):  # type: ignore
    if index <= 0:
        raise ValueError("Index must be a positive value")
    if index > _MAX_BUILT_IN_INDEX:
        raise ValueError("Built in index must be less than 100")

    decorated = _decorate_func(key=index, name=f"builtin.{name}", wrapper_dict=_INDEXED_AGGREGATORS, clazz=_Aggregator)

    return decorated


NUM = TypeVar("NUM", float, int)


@_typed_aggregator(mtype=float, name="number")
@_typed_aggregator(mtype=int, name="number")
def _sum(lhs: NUM, rhs: NUM) -> NUM:
    return lhs + rhs


@_typed_aggregator(mtype=ds.kll_doubles_sketch, name="kll")
def _kll_merge(lhs: ds.kll_doubles_sketch, rhs: ds.kll_doubles_sketch) -> ds.kll_doubles_sketch:
    kll_copy = ds.kll_doubles_sketch(lhs)
    kll_copy.merge(rhs)

    return kll_copy


@_typed_aggregator(mtype=ds.hll_sketch, name="kll")
def _hll_merge(lhs: ds.hll_sketch, rhs: ds.hll_sketch) -> ds.hll_sketch:
    lg_k = max(lhs.lg_config_k, rhs.lg_config_k)
    tg_type = lhs.tgt_type
    if tg_type.value < rhs.tgt_type.value:
        tg_type = rhs.tgt_type

    copy = ds.hll_union(lg_k)
    copy.update(lhs)
    copy.update(rhs)

    return copy.get_result(tg_type)


@_typed_aggregator(mtype=ds.frequent_strings_sketch, name="kll")
def _fs_merge(lhs: ds.frequent_strings_sketch, rhs: ds.frequent_strings_sketch) -> ds.frequent_strings_sketch:
    copy = ds.frequent_strings_sketch.deserialize(lhs.serialize())
    copy.merge(rhs)

    return copy


class AggregatorRegistry:
    def __init__(self) -> None:
        self._indexed_aggs: Dict[int, _Aggregator] = _INDEXED_AGGREGATORS.copy()
        self._typed_aggs = _TYPED_AGGREGATORS.copy()

    def get(self, *, index: int = 0, mtype: Optional[type] = None) -> Optional[_Aggregator]:
        if mtype is None and index <= 0:
            raise ValueError("index or metric component type must be set")
        result = None
        if index > 0:
            result = self._indexed_aggs.get(index)
        if result is None and mtype:
            result = self._typed_aggs.get(mtype)
        return result


_STANDARD_REGISTRY = AggregatorRegistry()


def aggregator(*, index: int, registry: Optional[AggregatorRegistry] = None):  # type: ignore
    if index < _MAX_BUILT_IN_INDEX:
        raise ValueError("Custom aggregator identifier must be equal or greater than 100")

    if registry is None:
        registry = _STANDARD_REGISTRY

    decorated = _decorate_func(
        key=index, name=f"custom.{index}", wrapper_dict=registry._indexed_aggs, clazz=_Aggregator
    )

    return decorated


def get_aggregator(
    *, index: int = 0, mtype: Optional[type] = None, registry: Optional[AggregatorRegistry] = None  # force kwargs
) -> Optional[_Aggregator]:
    if registry is None:
        registry = _STANDARD_REGISTRY

    return registry.get(index=index, mtype=mtype)
