import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import reduce
from glob import glob

import whylogs as why


@dataclass
class DateConfig:
    base_dir: str
    base_name: str
    start_date: datetime
    end_date: datetime


def _get_date_range(date_config: DateConfig):
    for n in range(int((date_config.end_date - date_config.start_date).days) + 1):
        yield date_config.start_date + timedelta(n)


def _get_dates_list(date_config: DateConfig):
    dates_list = []
    for single_date in _get_date_range(date_config=date_config):
        dates_list.append(single_date.strftime("%Y-%m-%d"))
    return dates_list


def read_profiles(date_config: DateConfig):
    dates_list = _get_dates_list(date_config=date_config)
    files_list = []
    for date in dates_list:
        files_list.extend(glob(f"{os.path.join(date_config.base_dir, date_config.base_name)}.{date}*.bin"))
    if len(files_list) > 0:
        profiles_list = [why.read(file).view() for file in files_list]
        merged_profile = reduce(lambda x, y: x.merge(y), profiles_list)
        return merged_profile
    else:
        return None


# if __name__ == "__main__":
#     read_profiles(
#         base_dir="whylogs_output",
#         base_name="test_base_name",
#         start_date=datetime(2022, 9, 6),
#         end_date=datetime(2022, 9, 9),
#     ).to_pandas()
