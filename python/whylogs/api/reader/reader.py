from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from whylogs import ResultSet

import whylogs as why

T = TypeVar("T", bound="Reader")


class Reader(ABC):
    @abstractmethod
    def read(self, **kwargs: Any) -> "ResultSet":
        """
        Must be implemented by all inherited Readers, declaring
        how to fetch files from their locations to a local temp dir
        """

    @abstractmethod
    def option(self: T, **kwargs: Any) -> T:
        pass

    @staticmethod
    def get_file_from_path(path) -> "ResultSet":
        return why.read(path)
