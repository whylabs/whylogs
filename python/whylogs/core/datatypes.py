from abc import ABC, abstractmethod
from typing import Any, Generic, List, Optional, Type, TypeVar, Union

from whylogs.core.stubs import np

try:
    from pandas.core.api import CategoricalDtype
except:  # noqa
    CategoricalDtype = None  # type: ignore

NT = TypeVar("NT")


class DataType(ABC, Generic[NT]):
    def __init__(self, tpe: Type[NT]):
        self._tpe = tpe

    @property
    def return_type(self) -> Type[NT]:
        return self._tpe

    @classmethod
    def match(cls, dtype_or_type: Any) -> bool:
        if dtype_or_type is None:
            return False
        if hasattr(dtype_or_type, "type"):
            maybe_type = dtype_or_type.type  # type: ignore
        else:
            maybe_type = None

        return cls._do_match(dtype_or_type, maybe_type)

    @classmethod
    @abstractmethod
    def _do_match(cls, dtype_or_type: Any, maybe_type: Optional[Any]) -> bool:
        raise NotImplementedError


NumericalType = Union[int, float, np.number]

NUMBER = TypeVar("NUMBER")


class Integral(DataType[int]):
    def __init__(self) -> None:
        super().__init__(int)

    @classmethod
    def _do_match(cls, dtype_or_type: Any, maybe_type: Optional[Any]) -> bool:
        if maybe_type:
            dtype_or_type = maybe_type  # type: ignore

        if not isinstance(dtype_or_type, type):
            return False

        if issubclass(dtype_or_type, (bool, int, np.number)):
            if np.issubdtype and np.issubdtype(dtype_or_type, np.floating):
                return False
            if issubclass(dtype_or_type, (np.datetime64, np.timedelta64)):
                return False
            else:
                return True
        return False


class Fractional(DataType[float]):
    def __init__(self) -> None:
        super().__init__(float)

    @classmethod
    def _do_match(cls, dtype_or_type: Any, maybe_type: Optional[Any]) -> bool:
        if maybe_type:
            dtype_or_type = maybe_type

        if not isinstance(dtype_or_type, type):
            return False

        return issubclass(dtype_or_type, (float, np.floating))


class String(DataType[str]):
    def __init__(self) -> None:
        super().__init__(str)

    @classmethod
    def _do_match(cls, dtype_or_type: Any, maybe_type: Optional[Any]) -> bool:
        # Pandas Categorical is Strings
        if isinstance(dtype_or_type, CategoricalDtype):
            return True

        # handle pandas series. Anything 'object' is treated as a Series of strings
        if hasattr(dtype_or_type, "name"):
            if dtype_or_type.name == "object":  # type: ignore
                return True

        # use the underlying type within a dtype object
        if maybe_type:
            dtype_or_type = maybe_type

        if not isinstance(dtype_or_type, type):
            return False

        if issubclass(dtype_or_type, (str, np.unicode_)):
            return True

        return False


class AnyType(DataType[Any]):
    def __init__(self) -> None:
        super().__init__(type)

    def is_compatible(self, value: Any) -> bool:
        return True

    @classmethod
    def _do_match(cls, dtype_or_type: Any, maybe_type: Optional[Any]) -> bool:
        return True


class TypeMapper(ABC):
    @abstractmethod
    def __call__(self, dtype_or_type: Any) -> DataType:
        raise NotImplementedError


class StandardTypeMapper(TypeMapper):
    """Map a dtype (Pandas) or a Python type to a data type."""

    def __init__(self, custom_types: Optional[List[Type[DataType]]] = None):
        """

        Args:
            custom_types: List of additional DataType classes that you want to extend.
        """
        self._all_types = [Integral, Fractional, String]
        if custom_types is not None:
            self._all_types.extend(custom_types)

    def __call__(self, dtype_or_type: Any) -> DataType:
        for clz in self._all_types:
            if clz.match(dtype_or_type):  # type: ignore
                return clz()  # type: ignore
        return AnyType()
