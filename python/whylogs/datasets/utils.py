import os
import re
from datetime import date, datetime
from typing import Tuple, Union

import pandas as pd

from whylogs.datasets.base import Batch
from whylogs.datasets.configs import BaseConfig, DatasetConfig

base_config = BaseConfig()


def _adjust_date(current_date, date_offset):
    adjusted_date = datetime.strptime(current_date, "%Y-%m-%d").date() + date_offset

    return adjusted_date


def _parse_interval(interval: str) -> Tuple[int, str]:
    try:
        result = re.findall(r"^(\d+)([DdMmHhSs])", interval)[0]
        return (int(result[0]), result[1].upper())
    except:
        raise ValueError("Could not parse interval!")


def _change_df_date_by_offset(df, new_start_date):
    original_start_date = df["date"][0]

    if isinstance(original_start_date, str):
        original_start_date = datetime.strptime(original_start_date, "%Y-%m-%d").date()
        date_offset = new_start_date - original_start_date
        df["date"] = df["date"].apply(lambda x: _adjust_date(x, date_offset))
        return df

    elif not isinstance(original_start_date, (date, datetime)):
        raise ValueError("Date column must be either valid string format, date or datetime types.")

    date_offset = new_start_date - original_start_date
    df["date"] = df["date"].apply(lambda x: x + date_offset)
    return df


def _validate_timestamp(timestamp):
    if isinstance(timestamp, datetime):
        return timestamp.date()
    elif isinstance(timestamp, date):
        return timestamp
    else:
        raise ValueError("You must pass either a Datetime or Date object to timestamp!")


def _get_data_home():
    home_path = os.path.join(".", base_config.data_folder)
    if not os.path.exists(home_path):
        os.makedirs(home_path)
    return home_path


def _get_dataset_path(folder_name):
    home_path = _get_data_home()
    dataset_path = os.path.join(home_path, folder_name)
    if not os.path.exists(dataset_path):
        os.makedirs(dataset_path)
    return dataset_path
