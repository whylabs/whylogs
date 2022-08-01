from datetime import date, datetime
import re
from typing import Tuple, Union
import pandas as pd
from whylogs.datasets.configs import DatasetConfig
from whylogs.datasets.base import Batch


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


def assemble_batch_from_df(
    data: pd.DataFrame, version: str, dataset_config: DatasetConfig, timestamp: Union[date, datetime]
):
    target_df = data[list(dataset_config.target_columns[version])]
    predictions_df = data[list(dataset_config.prediction_columns[version])]
    metadata_df = data[list(dataset_config.metadata_columns[version])]
    features_df = data.drop(columns=list(target_df.columns) + list(predictions_df.columns) + list(metadata_df.columns))
    batch = Batch(
        timestamp=timestamp,
        data=data,
        features=features_df,
        target=target_df,
        prediction=predictions_df,
        meta=metadata_df,
    )
    return batch
