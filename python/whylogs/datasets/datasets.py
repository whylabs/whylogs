from abc import ABC, abstractmethod
from dataclasses import dataclass
from distutils.command.config import config
from email.mime import base
from math import prod
from multiprocessing.sharedctypes import Value
from time import time
from typing import Any, Optional, Union, Iterable
from click import DateTime

from numpy import ndarray, number
from whylogs.datasets.configs import WeatherConfig
from datetime import date, datetime
import logging
from typing_extensions import TypedDict
import pandas as pd
from whylogs.datasets.utils import _change_df_date_by_offset, _validate_timestamp

logger = logging.getLogger(__name__)


@dataclass
class Batch:
    timestamp: date
    frame: pd.DataFrame
    DESCR: Optional[str] = None


class Dataset(ABC):
    @staticmethod
    @abstractmethod
    def describe_versions() -> "list[str]":
        raise NotImplementedError

    @abstractmethod
    def set_parameters(
        self,
        interval: str,
        baseline_timestamp: Optional[Union[date, datetime]] = None,
        inference_start_timestamp: Optional[Union[date, datetime]] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_baseline(self) -> Batch:
        raise NotImplementedError


@dataclass(init=False)
class Weather(Dataset):
    """Weather Forecast Dataset

    Number of instances:
    Number of attributes:

    This dataset is based on data available at https://github.com/Shifts-Project/shifts
    """

    interval: str = "1d"
    url = WeatherConfig.url
    baseline_timestamp: Union[date, datetime] = WeatherConfig.baseline_start_timestamp
    inference_start_timestamp: Union[date, datetime] = WeatherConfig.inference_start_timestamp
    baseline_df: Optional[pd.DataFrame] = None
    inference_df: Optional[pd.DataFrame] = None

    def __init__(self, version: Optional[str] = "in_domain") -> None:
        if version not in WeatherConfig.available_versions:
            raise ValueError("Version not found in list of available versions.")
        self.version = version
        self.baseline_df = pd.read_csv("{}/baseline_dataset.csv".format(self.url))
        self.inference_df = pd.read_csv("{}/inference_dataset_{}.csv".format(self.url, self.version))

    def get_baseline(self) -> Batch:
        baseline = Batch(timestamp=self.baseline_timestamp, frame=self.baseline_df)
        return baseline

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
            inference = Batch(timestamp=target_date, frame=self.inference_df.loc[mask])
            return inference
        raise ValueError("Target date should be either of date or datetime type.")

    def set_parameters(
        self,
        interval: str,
        baseline_timestamp: Optional[Union[date, datetime]] = None,
        inference_start_timestamp: Optional[Union[date, datetime]] = None,
    ) -> None:
        if interval != "1d":
            raise ValueError("Input interval not supported!")
        self.interval = interval

        if baseline_timestamp:
            self.baseline_timestamp = _validate_timestamp(baseline_timestamp)
        if inference_start_timestamp:
            self.inference_start_timestamp = _validate_timestamp(inference_start_timestamp)
            self.inference_df = _change_df_date_by_offset(self.inference_df, self.inference_start_timestamp)

    @staticmethod
    def describe_versions():
        available_versions = WeatherConfig.available_versions
        return available_versions

    @classmethod
    def describe(cls):
        return cls.__doc__
