from typing import Callable, Generic, Optional, TypeVar

T = TypeVar("T")


class Lazy(Generic[T]):
    def __init__(self, fn: Callable[[], T]) -> None:
        self.fn = fn
        self.__value: Optional[T] = None

    @property
    def value(self) -> T:
        if self.__value is None:
            self.__value = self.fn()
        return self.__value
