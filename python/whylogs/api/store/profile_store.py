from abc import ABC, abstractmethod
from datetime import timedelta
from typing import List, Optional

from whylogs.api.store.query import DateQuery
from whylogs.core import DatasetProfileView


class ProfileStore(ABC):
    @abstractmethod
    def get(self, query: DateQuery) -> DatasetProfileView:
        pass

    @abstractmethod
    def write(self, profile: Optional[DatasetProfileView]) -> None:
        pass

    @staticmethod
    def _get_date_range(query: DateQuery):
        for n in range(int((query.end_date - query.start_date).days) + 1):
            yield query.start_date + timedelta(n)

    # TODO return datetimes instead of strings
    def _get_dates(self, query: DateQuery) -> List[str]:
        dates = []
        for single_date in self._get_date_range(query=query):
            dates.append(single_date.strftime("%Y-%m-%d"))
        return dates
