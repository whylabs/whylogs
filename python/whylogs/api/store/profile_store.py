from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Generator, List

from whylogs.api.store.query import BaseQuery, DateQuery
from whylogs.core import DatasetProfileView


class ProfileStore(ABC):
    @abstractmethod
    def list(self) -> List[str]:
        pass

    @abstractmethod
    def get(self, query: BaseQuery) -> DatasetProfileView:
        pass

    # TODO this has to be updated to be able to handle more types of things. It might be better to just
    # accept any Writable instead of the concrete types, like Writer does.
    @abstractmethod
    def write(self, profile_view: DatasetProfileView, dataset_id: str) -> None:
        pass

    @staticmethod
    def _get_date_range(query: DateQuery) -> Generator[datetime, None, None]:
        if query.end_date is None:
            yield query.start_date
            return

        for n in range(int((query.end_date - query.start_date).days) + 1):
            yield query.start_date + timedelta(n)

    def _get_dates(self, query: DateQuery) -> List[datetime]:
        dates = []
        for single_date in self._get_date_range(query=query):
            dates.append(single_date)
        return dates
