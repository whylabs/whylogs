import os
from abc import ABC, abstractmethod
from typing import Any, Optional, TypeVar

from whylogs.core import DatasetProfileView

T = TypeVar("T", bound="Writer")


class Writable(ABC):
    @staticmethod
    def _safe_open_write(path):
        """Open `path` for writing, creating any parent directories as needed."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return open(path, 'w')

    @abstractmethod
    def write(self, path: Optional[str] = None, **kwargs: Any):
        pass


class Writer(Writable, ABC):

    # noinspection PyMethodMayBeStatic
    def check_interval(self, interval_seconds: int) -> None:
        """Validate an interval configuration for a given writer.

        Some writer only accepts certain interval configuration. By default, this should return True for a valid
        non-negative interval."""

    @abstractmethod
    def write(
        self, 
        file: Optional[Writable] = None, 
        profile: Optional[DatasetProfileView] = None, 
        dest: Optional[str] = None
        ) -> None:
        pass

    @abstractmethod
    def option(self: T, **kwargs: Any) -> T:
        pass
