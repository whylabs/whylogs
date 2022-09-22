from abc import ABC, abstractmethod
from typing import Optional

from whylogs.api.store.query import StoreQuery
from whylogs.core import DatasetProfileView


class ProfileStore(ABC):
    @abstractmethod
    def get(self, date_config: StoreQuery) -> DatasetProfileView:
        pass

    @abstractmethod
    def write(self, profile: Optional[DatasetProfileView]) -> None:
        pass
