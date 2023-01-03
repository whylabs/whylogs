from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List

from whylogs.api.store.query import BaseQuery, DateQuery
from whylogs.core import DatasetProfileView


class ProfileStore(ABC):
    @abstractmethod
    def list(self) -> List[str]:
        pass

    @abstractmethod
    def get(self, query: BaseQuery) -> DatasetProfileView:
        pass

    @abstractmethod
    def write(self, profile_view: DatasetProfileView, dataset_id: str) -> None:
        pass

    @staticmethod
    def _get_date_range(query: DateQuery):
        for n in range(int((query.end_date - query.start_date).days) + 1):
            yield query.start_date + timedelta(n)

    def _get_dates(self, query: DateQuery) -> List[datetime]:
        dates = []
        for single_date in self._get_date_range(query=query):
            dates.append(single_date)
        return dates
