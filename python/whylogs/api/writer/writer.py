from abc import ABC, abstractmethod
from typing import Any, Optional, TypeVar

from whylogs.core import DatasetProfileView

T = TypeVar("T", bound="Writer")


class Writer(ABC):
    @abstractmethod
    def write(self, profile: DatasetProfileView, dest: Optional[str] = None) -> None:
        pass

    @abstractmethod
    def option(self: T, **kwargs: Any) -> T:
        pass
