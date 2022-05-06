from dataclasses import dataclass
from typing import Generic, Optional, Type, TypeVar

import whylogs_sketching as ds  # type: ignore

from whylogs.core.metrics.aggregators import (
    AggregatorRegistry,
    _id_aggregator,
    get_aggregator,
)
from whylogs.core.metrics.deserializers import DeserializerRegistry, get_deserializer
from whylogs.core.metrics.serializers import SerializerRegistry, get_serializer
from whylogs.core.proto import MetricComponentMessage

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

    A metric might consist of multiple components. An example is distribution metric, which consists of kll sketch for
    histogram, mean and m2. The calculation of components could be independent or could be coupled with other
    components.
    """

    mtype: Optional[Type[T]]
    type_id: int = 0
    registries: Optional[Registries] = None

    def __init__(self, value: T):
        self._value = value

        registries = self.registries
        if registries is None:
            registries = Registries()

        self._registries = registries
        optional_mtype = None
        if hasattr(self, "mtype"):
            optional_mtype = self.mtype
        self._aggregator = get_aggregator(
            mtype=optional_mtype, type_id=self.type_id, registry=registries.aggregatorRegistry
        )
        self._serializer = get_serializer(
            mtype=optional_mtype, type_id=self.type_id, registry=registries.serializerRegistry
        )
        self._deserializer = get_deserializer(
            mtype=optional_mtype, type_id=self.type_id, registry=registries.deserializerRegistry
        )

        if self._serializer is None and self._deserializer is not None:
            raise ValueError("Serializer and deserializer must be defined in pairs, but serializer is None")
        if self._serializer is not None and self._deserializer is None:
            raise ValueError("Serializer and deserializer must be defined in pairs, but deserializer is None")

    @property
    def value(self) -> T:
        return self._value

    def set(self, value: T) -> None:
        self._value = value

    def __add__(self: M, other: M) -> M:
        if self._aggregator is None:
            raise ValueError(
                f"Attempting to aggregate metric component without an aggregator. Type: {self.mtype} with ID: "
                f"{self.type_id}"
            )
        return self.__class__(self._aggregator(self.value, other.value))

    def to_protobuf(self) -> MetricComponentMessage:
        if self._serializer is None:
            raise ValueError(
                f"Attempting to serialize metric component without a serializer. Type: {self.mtype} with ID: "
                f"{self.type_id}"
            )
        msg = self._serializer(value=self._value)
        msg.type_id = self.type_id
        return msg

    @classmethod
    def from_protobuf(cls: Type["M"], msg: MetricComponentMessage, registries: Optional[Registries] = None) -> "M":
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
        _type_id = msg.type_id

        _registries = registries

        class DeserializedComponent(MetricComponent[M]):
            mtype = _mtype
            type_id = msg.type_id
            registries = _registries

        component = DeserializedComponent(value=None)  # type: ignore
        if component._deserializer is not None:
            component._value = component._deserializer(msg=msg)
            return component  # type: ignore
        else:
            raise ValueError(f"Attempt to deserialize a type without a deserializer. Type: {_mtype}. ID: {_type_id}")


class IntegralComponent(MetricComponent[int]):
    mtype = int


class MinIntegralComponent(MetricComponent[int]):
    mtype = int
    type_id = 1

    @staticmethod
    @_id_aggregator(type_id=type_id, name="min")
    def min(lhs: int, rhs: int) -> int:
        return min([lhs, rhs])


class MaxIntegralComponent(MetricComponent[int]):
    mtype = int
    type_id = 2

    @staticmethod
    @_id_aggregator(type_id=type_id, name="max")
    def max(lhs: int, rhs: int) -> int:
        return max([lhs, rhs])


class FractionalComponent(MetricComponent[float]):
    mtype = float


class KllComponent(MetricComponent[ds.kll_doubles_sketch]):
    mtype = ds.kll_doubles_sketch


class HllComponent(MetricComponent[ds.hll_sketch]):
    mtype = ds.hll_sketch


class FrequentStringsComponent(MetricComponent[ds.frequent_strings_sketch]):
    mtype = ds.frequent_strings_sketch


class CustomComponent(Generic[T], MetricComponent[T]):
    type_id = -1

    def __init__(self, value: T) -> None:
        if self.type_id < 100:
            raise ValueError("Custom metric component must have type index greater or equal to 100")
        super().__init__(value)
