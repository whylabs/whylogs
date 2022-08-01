from curses import meta
from dataclasses import dataclass
from tkinter.tix import Meter
from typing import Optional, Union, Iterable

from importlib_metadata import metadata
from whylogs.datasets.configs import DatasetConfig, WeatherConfig
from datetime import date, datetime, timedelta
from whylogs.datasets.base import Batch, Dataset
import pandas as pd
from whylogs.datasets.utils import (
    _change_df_date_by_offset,
    _validate_timestamp,
    _parse_interval,
    assemble_batch_from_df,
)
from logging import getLogger

logger = getLogger(__name__)


@dataclass(init=False)
class Weather(Dataset):
    """Weather Forecast Dataset

    Number of instances:
    Number of attributes:

    This dataset is based on data available at https://github.com/Shifts-Project/shifts
    """

    inference_interval: str = "1d"
    number_days: int = 1
    unit: str = "D"
    url: str = WeatherConfig.url
    baseline_timestamp: Union[date, datetime] = date.today()
    inference_start_timestamp: Union[date, datetime] = date.today() + timedelta(1)
    baseline_df: Optional[pd.DataFrame] = None
    inference_df: Optional[pd.DataFrame] = None
    original: bool = False
    dataset_config: DatasetConfig = WeatherConfig()

    def __init__(self, version: Optional[str] = "in_domain") -> None:
        if version not in self.dataset_config.available_versions:
            raise ValueError("Version not found in list of available versions.")
        self.version = version
        self.baseline_df = pd.read_csv("{}/baseline_dataset.csv".format(self.url))
        self.inference_df = pd.read_csv("{}/inference_dataset_{}.csv".format(self.url, self.version))
        self.inference_df = self.inference_df.set_index(["date"], drop=False)
        self.inference_df = _change_df_date_by_offset(
            self.inference_df, new_start_date=self.inference_start_timestamp
        )  # To cast date column from string to date

    def get_baseline(self) -> Batch:
        data = self.baseline_df
        baseline = assemble_batch_from_df(data, self.version, self.dataset_config, self.baseline_timestamp)
        return baseline

    def _validate_interval(self, interval):
        number_days, unit = _parse_interval(interval)
        if number_days > self.dataset_config.max_interval:
            raise ValueError("Maximum allowed interval for this dataset is {}".format(self.dataset_config.max_interval))
        if unit != "D":
            raise ValueError("Current accepted unit for this dataset is {}".format(self.dataset_config.base_unit))
        return (number_days, unit)

    def get_inference_data(
        self, target_date: Optional[Union[date, datetime]] = None, number_batches: Optional[int] = None
    ) -> Union[Batch, Iterable[Batch]]:
        if not target_date and not number_batches:
            raise ValueError("date or number_batches must be passed to get_inference_data.")
        if target_date and number_batches:
            raise ValueError("Either date or number_batches should be passed, not both.")
        if target_date and isinstance(target_date, (date, datetime)):
            target_date = _validate_timestamp(target_date)
            mask = self.inference_df["date"] == target_date
            data = self.inference_df.loc[mask]
            inference = assemble_batch_from_df(data, self.version, self.dataset_config, target_date)
            return inference
        if number_batches:
            batches = WeatherDatasetIterator(
                self.inference_df,
                number_days=self.number_days,
                number_batches=number_batches,
                version=self.version,
                config=self.dataset_config,
            )
            return batches
        raise ValueError("Target date should be either of date or datetime type.")

    def set_parameters(
        self,
        inference_interval: Optional[str] = None,
        baseline_timestamp: Optional[Union[date, datetime]] = None,
        inference_start_timestamp: Optional[Union[date, datetime]] = None,
        original: Optional[bool] = None,
    ) -> None:
        if inference_interval:
            self.inference_interval = inference_interval
            self.number_days, self.unit = self._validate_interval(self.inference_interval)
        if original:
            self.baseline_timestamp = self.dataset_config.baseline_start_timestamp
            self.inference_start_timestamp = self.dataset_config.inference_start_timestamp
        if baseline_timestamp:
            if not original:
                self.baseline_timestamp = _validate_timestamp(baseline_timestamp)
            else:
                logger.warning(
                    "baseline_timestamp and inference_start_timestamp overriden by original timestamps due to original = True"
                )
        if inference_start_timestamp:
            if not original:
                self.inference_start_timestamp = _validate_timestamp(inference_start_timestamp)
            else:
                logger.warning(
                    "baseline_timestamp and inference_start_timestamp overriden by original timestamps due to original = True"
                )
            self.inference_df = _change_df_date_by_offset(self.inference_df, self.inference_start_timestamp)
            self.inference_df = self.inference_df.set_index(["date"], drop=False)

    @classmethod
    def describe_versions(cls):
        available_versions = cls.dataset_config.available_versions
        return available_versions

    @classmethod
    def describe(cls):
        return cls.__doc__


class WeatherDatasetIterator:
    def __init__(self, df: pd.DataFrame, number_days: int, number_batches: int, version: str, config=DatasetConfig):
        self._df = df
        self._number_days = number_days
        self._number_batches = number_batches
        self.version = version
        self.config = config

    def __iter__(self):
        self._index: date = self._df.iloc[0].name
        self._batch_counter: int = 0
        return self

    def __next__(self):
        if self._batch_counter >= self._number_batches:
            raise StopIteration
        if self._index <= self._df.iloc[-1].name:
            day = self._index
            data = self._df[day : day + timedelta(days=(self._number_days - 1))]
            inference = assemble_batch_from_df(data, self.version, self.config, day)
            self._index = data.index[-1] + timedelta(days=1)
            self._batch_counter += 1

            return inference

        raise StopIteration
