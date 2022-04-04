from abc import ABC, abstractmethod
from typing import Any, Generic, List, Type, TypeVar, Union

from whylogs_v1.core.common import COMMON_COLUMNAR_TYPES
from whylogs_v1.core.utils import numpy as np
from whylogs_v1.core.utils import pandas as pd

IT = TypeVar("IT")
NT = TypeVar("NT")


class DataType(ABC, Generic[IT, NT]):
    @abstractmethod
    def is_compatible(self, value: Any) -> bool:
        raise NotImplementedError

    @abstractmethod
    def normalize(self, value: IT) -> NT:
        raise NotImplementedError

    def batch_normalize(
        self, values: COMMON_COLUMNAR_TYPES
    ) -> Union[List[NT], pd.Series, np.ndarray]:
        return list(map(self.normalize, values))


NumericalType = Union[int, float]

N = TypeVar("N", float, int)


class NumericColumnType(DataType[NumericalType, N], Generic[N]):
    _type: Type[N]

    def __init__(self, tpe: Type[N]):
        self._type = tpe

    def is_compatible(self, value: Any) -> bool:
        return isinstance(value, (int, float))

    def normalize(self, value: NumericalType) -> N:
        return self._type(value)  # type: ignore


class AnyType(DataType):
    def is_compatible(self, value: Any) -> bool:
        return True

    def normalize(self, value: Any) -> Any:
        return value
