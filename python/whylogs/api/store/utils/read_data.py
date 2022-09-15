from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class DateConfig:
    base_name: str
    start_date: datetime
    end_date: datetime


def _get_date_range(date_config: DateConfig):
    for n in range(int((date_config.end_date - date_config.start_date).days) + 1):
        yield date_config.start_date + timedelta(n)


def get_dates_list(date_config: DateConfig):
    dates_list = []
    for single_date in _get_date_range(date_config=date_config):
        dates_list.append(single_date.strftime("%Y-%m-%d"))
    return dates_list
