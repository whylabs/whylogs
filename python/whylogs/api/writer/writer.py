from abc import ABC, abstractmethod
from typing import Any, Optional, TypeVar

from whylogs.core import DatasetProfileView

T = TypeVar("T", bound="Writer")


class Writer(ABC):

    # noinspection PyMethodMayBeStatic
    def check_interval(self, interval_seconds: int) -> None:
        """Validate an interval configuration for a given writer.

        Some writer only accepts certain interval configuration. By default, this should return True for a valid
        non-negative interval."""

    @abstractmethod
    def write(self, profile: DatasetProfileView, dest: Optional[str] = None) -> None:
        pass

    @abstractmethod
    def option(self: T, **kwargs: Any) -> T:
        pass
