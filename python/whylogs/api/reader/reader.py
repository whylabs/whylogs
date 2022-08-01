from abc import ABC, abstractmethod
from typing import Any, TypeVar

T = TypeVar("T", bound="Reader")


class Reader(ABC):
    @abstractmethod
    def read(
        self,
        path: str,
        **kwargs: Any,
    ) -> None:
        pass

    @abstractmethod
    def option(self: T, **kwargs: Any) -> T:
        pass
