import os
from abc import ABC, abstractmethod
from typing import Any, Optional, TypeVar

T = TypeVar("T", bound="Writer")


class Writable(ABC):
    @staticmethod
    def _safe_open_write(path):
        """Open `path` for writing, creating any parent directories as needed."""
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        except FileExistsError:
            pass
        return open(path, "w")

    @abstractmethod
    def get_default_path(self) -> str:
        pass

    @abstractmethod
    def write(self, path: Optional[str] = None, **kwargs: Any) -> None:
        pass


class Writer(ABC):
    # noinspection PyMethodMayBeStatic
    def check_interval(self, interval_seconds: int) -> None:
        """Validate an interval configuration for a given writer.

        Some writer only accepts certain interval configuration. By default, this should return True for a valid
        non-negative interval."""

    @abstractmethod
    def write(
        self,
        file: Writable,
        dest: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        pass

    @abstractmethod
    def option(self: T, **kwargs: Any) -> T:
        pass
