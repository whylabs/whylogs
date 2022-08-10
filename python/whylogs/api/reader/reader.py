from abc import ABC, abstractmethod
from typing import Any, TypeVar

from whylogs import ResultSet

T = TypeVar("T", bound="Reader")


class Reader(ABC):
    @abstractmethod
    def read(self, **kwargs: Any) -> ResultSet:
        """
        Must be implemented by all inherited Readers, declaring
        how to fetch files from their locations to a local temp dir
        """
        pass

    @abstractmethod
    def option(self: T, **kwargs: Any) -> T:
        pass

    @staticmethod
    def get_file_from_path(path) -> ResultSet:
        return ResultSet.read(path)
