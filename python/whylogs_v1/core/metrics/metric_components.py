from dataclasses import dataclass
from typing import Generic, Optional, Type, TypeVar

import whylogs_datasketches as ds  # type: ignore

from whylogs_v1.core.metrics.aggregators import (
    AggregatorRegistry,
    _indexed_aggregator,
    get_aggregator,
)
from whylogs_v1.core.metrics.deserializers import DeserializerRegistry, get_deserializer
from whylogs_v1.core.metrics.serializers import SerializerRegistry, get_serializer
from whylogs_v1.core.proto import MetricComponentMessage

T = TypeVar("T")
M = TypeVar("M", bound="MetricComponent")


@dataclass(frozen=True)
class Registries:
    aggregatorRegistry: Optional[AggregatorRegistry] = None
    serializerRegistry: Optional[SerializerRegistry] = None
    deserializerRegistry: Optional[DeserializerRegistry] = None


class MetricComponent(Generic[T]):
    """
    A metric component is the smallest unit for a metric.

    A metric might consist of multiple component. An example is distribution metric, which consists of kll sketch for
    histogram, mean and m2. The calculation of components could be independent or could be coupled with other
    components.
    histogram, mean and m2. The calculation of components could be independent or could be coupled with other
    components.
    """

    mtype: Optional[Type[T]]
    index: int = 0
    registries: Optional[Registries] = None

    def __init__(self, value: T):
        self._value = value

        registries = self.registries
        if registries is None:
            registries = Registries()

        self._registries = registries
        self._aggregator = get_aggregator(mtype=self.mtype, index=self.index, registry=registries.aggregatorRegistry)
        self._serializer = get_serializer(mtype=self.mtype, index=self.index, registry=registries.serializerRegistry)
        self._deserializer = get_deserializer(
            mtype=self.mtype, index=self.index, registry=registries.deserializerRegistry
        )

        if self._serializer is None and self._deserializer is not None:
            raise ValueError("Serializer and deserializer must be both defined")
        if self._serializer is not None and self._deserializer is None:
            raise ValueError("Serializer and deserializer must be both defined")

    @property
    def value(self) -> T:
        return self._value

    def set(self, value: T) -> None:
        self._value = value

    def __add__(self: M, other: M) -> M:
        if self._aggregator is None:
            raise ValueError(
                f"Attempting to aggregate metric component without an aggregator. Type: {self.mtype} with index: "
                f"{self.index}"
            )
        return self.__class__(self._aggregator(self.value, other.value))

    def serialize(self) -> MetricComponentMessage:
        if self._serializer is None:
            raise ValueError(
                f"Attempting to serialize metric component without a serializer. Type: {self.mtype} with "
                f"index: "
                f"{self.index}"
            )
        msg = self._serializer(value=self._value)
        msg.index = self.index
        return msg

    @classmethod
    def deserialize(cls: Type["M"], msg: MetricComponentMessage, registries: Optional[Registries] = None) -> "M":
        # TODO: need to deserialize this against a registry
        field = msg.WhichOneof("value")
        _mtype: Optional[type] = None
        if field == "n":
            _mtype = int
        elif field == "d":
            _mtype = float
        elif field == "frequent_items":
            _mtype = ds.frequent_strings_sketch
        elif field == "hll":
            _mtype = ds.hll_sketch
        elif field == "kll":
            _mtype = ds.kll_doubles_sketch
        _index = msg.index

        _registries = registries

        class DeserializedComponent(MetricComponent[M]):
            mtype = _mtype
            index = msg.index
            registries = _registries

        component = DeserializedComponent(value=None)  # type: ignore
        if component._deserializer is not None:
            component._value = component._deserializer(msg=msg)
            return component  # type: ignore
        else:
            raise ValueError(f"Attempt to deserialize a type without a deserializer. Type: {_mtype}. Index: {_index}")


class IntegralComponent(MetricComponent[int]):
    mtype = int


class MinIntegralComponent(MetricComponent[int]):
    mtype = int
    index = 1

    @staticmethod
    @_indexed_aggregator(index=index, name="min")
    def min(lhs: int, rhs: int) -> int:
        return min([lhs, rhs])


class MaxIntegralComponent(MetricComponent[int]):
    mtype = int
    index = 2

    @staticmethod
    @_indexed_aggregator(index=index, name="max")
    def max(lhs: int, rhs: int) -> int:
        return max([lhs, rhs])


class FractionalComponent(MetricComponent[float]):
    mtype = float


class KllComponent(MetricComponent[ds.kll_doubles_sketch]):
    mtype = ds.kll_doubles_sketch


class HllComponent(MetricComponent[ds.hll_sketch]):
    mtype = ds.hll_sketch


class FrequentItemsComponent(MetricComponent[ds.frequent_strings_sketch]):
    mtype = ds.frequent_strings_sketch


class CustomComponent(Generic[T], MetricComponent[T]):
    index = -1

    def __init__(self, value: T) -> None:
        if self.index < 100:
            raise ValueError("Custom metric component must have type index greater or equal to 100")
        super().__init__(value)
