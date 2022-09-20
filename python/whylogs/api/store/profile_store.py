from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Optional

from whylogs.api.store.date_config import DateConfig
from whylogs.core import DatasetProfileView


class ProfileStore(ABC):
    @abstractmethod
    def get(self, date_config: DateConfig) -> DatasetProfileView:
        pass

    @abstractmethod
    def write(self, profile: Optional[DatasetProfileView]) -> None:
        pass

    def _get_date_range(self, date_config: DateConfig):
        for n in range(int((date_config.end_date - date_config.start_date).days) + 1):
            yield date_config.start_date + timedelta(n)

    def get_dates_list(self, date_config: DateConfig):
        dates_list = []
        for single_date in self._get_date_range(date_config=date_config):
            dates_list.append(single_date.strftime("%Y-%m-%d"))
        return dates_list
