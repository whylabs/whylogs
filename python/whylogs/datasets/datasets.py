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
        production_start_timestamp: Optional[Union[date, datetime]] = None,
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
    production_start_timestamp: Union[date, datetime] = WeatherConfig.production_start_timestamp
    date_offset: int = 0

    def __init__(self, version: Optional[str] = "in_domain") -> None:
        if version not in WeatherConfig.available_versions:
            raise ValueError("Version not found in list of available versions.")
        self.version = version

    def get_baseline(self) -> Batch:
        df = pd.read_csv("{}/baseline_dataset.csv".format(self.url))
        timestamp = self.baseline_timestamp
        baseline = Batch(timestamp=timestamp, frame=df)
        return baseline

    def get_production_data(
        self, date: Optional[Union[date, datetime]] = None, number_batches: Optional[int] = None
    ) -> Union[Batch, Iterable[Batch]]:
        if not date and not number_batches:
            raise ValueError("date or number_batches must be passed to get_production_data.")
        if date and number_batches:
            raise ValueError("Either date or number_batches should be passed, not both.")
        if date:
            df = pd.read_csv("{}/inference_dataset_{}.csv".format(self.url, self.version))
            mask = df["date"] == date - self.date_offset
            df = df.loc[mask]
            timestamp = date
            inference_batch = Batch(timestamp=timestamp, frame=df)
            return inference_batch

        pass

    def set_parameters(
        self,
        interval: str,
        baseline_timestamp: Optional[Union[date, datetime]] = None,
        production_start_timestamp: Optional[Union[date, datetime]] = None,
    ) -> None:
        if interval != "1d":
            raise ValueError("Input interval not supported!")
        self.interval = interval

        if baseline_timestamp:
            if isinstance(baseline_timestamp, datetime):
                self.baseline_timestamp = baseline_timestamp.date()
            elif isinstance(baseline_timestamp, date):
                self.baseline_timestamp = baseline_timestamp
            else:
                raise ValueError("You must pass either a Datetime or Date object to baseline_timestamp!")
        if production_start_timestamp:
            if isinstance(production_start_timestamp, datetime):
                self.production_start_timestamp = production_start_timestamp.date()
            elif isinstance(production_start_timestamp, date):
                self.production_start_timestamp = production_start_timestamp
            else:
                raise ValueError("You must pass either a Datetime or Date object to production_start_timestamp!")
            self.date_offset = self.production_start_timestamp - WeatherConfig.production_start_timestamp

    @staticmethod
    def describe_versions():
        available_versions = WeatherConfig.available_versions
        return available_versions

    @classmethod
    def describe(cls):
        return cls.__doc__
