import os
import re
from datetime import date, datetime, timedelta
from typing import Tuple, Union

import pandas as pd

from whylogs.datasets.configs import BaseConfig

base_config = BaseConfig()


def _adjust_date(current_date: str, date_offset: timedelta) -> datetime:
    adjusted_date = _validate_timestamp(current_date) + date_offset
    return adjusted_date


def _parse_interval(interval: str) -> Tuple[int, str]:
    try:
        result = re.findall(r"^(\d+)([DdMmHhSs])", interval)[0]
        return (int(result[0]), result[1].upper())
    except IndexError:
        raise ValueError("Could not parse interval {}!".format(interval))


def _adjust_df_date(df: pd.DataFrame, new_start_date: datetime) -> pd.DataFrame:
    df = _change_df_date_by_offset(df, new_start_date=new_start_date)
    df = df.set_index(["date"], drop=False)
    return df


def _change_df_date_by_offset(df: pd.DataFrame, new_start_date: datetime) -> pd.DataFrame:
    original_start_date = _validate_timestamp(df["date"][0])
    date_offset = new_start_date - original_start_date
    df["date"] = df["date"].apply(lambda x: _adjust_date(x, date_offset))
    return df


def _validate_timestamp(timestamp: Union[date, datetime, pd.Timestamp, str]) -> datetime:
    if isinstance(timestamp, pd.Timestamp):
        return timestamp.to_pydatetime()
    if isinstance(timestamp, str):
        try:
            return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S%z")
        except ValueError:
            raise ValueError('Could not parse string as datetime. Expected format: "%Y-%m-%d %H:%M:%S%z"')
    if isinstance(timestamp, datetime):
        return timestamp
    if isinstance(timestamp, date):
        timestamp = datetime(
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
        )
        return timestamp
    raise ValueError("You must pass either a Datetime or Date object to timestamp!")


def _get_data_home() -> str:
    home_path = os.path.join(".", base_config.data_folder)
    if not os.path.exists(home_path):
        os.makedirs(home_path)
    return home_path


def _get_dataset_path(folder_name: str) -> str:
    home_path = _get_data_home()
    dataset_path = os.path.join(home_path, folder_name)
    if not os.path.exists(dataset_path):
        os.makedirs(dataset_path)
    return dataset_path
