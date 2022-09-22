from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Optional


class StoreQuery(ABC):
    @abstractmethod
    def query(self) -> List[str]:
        pass


class DateQuery(StoreQuery):
    def __init__(self, start_date: datetime, end_date: Optional[datetime] = None):
        self.start_date = start_date
        self.end_date = end_date

    def __post_init__(self):
        if self.end_date is None:
            self.end_date = self.start_date
        if self.end_date < self.start_date:
            self.start_date, self.end_date = self.end_date, self.start_date

    def _get_date_range(self):
        for n in range(int((self.end_date - self.start_date).days) + 1):
            yield self.start_date + timedelta(n)

    def _get_files_query(self) -> List[str]:
        files_query = []
        for single_date in self._get_date_range():
            files_query.append(f"profile_{single_date.strftime('%Y-%m-%d')}")
        return files_query

    def query(self) -> List[str]:
        files_query = self._get_files_query()
        return files_query


class ProfileIdQuery(StoreQuery):
    def query(self) -> List[str]:
        return ["profile"]
