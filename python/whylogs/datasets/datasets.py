from abc import ABC, abstractmethod
from dataclasses import dataclass
from email.mime import base
from math import prod
from multiprocessing.sharedctypes import Value
from typing import Any, Optional, Union

from numpy import ndarray
from whylogs.datasets.configs import WeatherConfig
from datetime import date, datetime
import logging
from typing_extensions import TypedDict
import pandas as pd

logger = logging.getLogger(__name__)


class Dataset(ABC):
    @staticmethod
    @abstractmethod
    def describe_versions() -> "list[str]":
        raise NotImplementedError


@dataclass
class Batch:
    timestamp: date
    frame: pd.DataFrame
    DESCR: str


@dataclass(init=False)
class Weather(Dataset):
    interval: str = "1d"
    url = WeatherConfig.available_versions
    baseline_timestamp: Optional[Union[date, datetime]] = None
    production_start_timestamp: Optional[Union[date, datetime]] = None

    def __init__(self, version: Optional[str] = "in_domain") -> None:
        if version not in WeatherConfig.available_versions:
            raise ValueError("Version not found in list of available versions.")
        self.version = version

    def get_baseline(self) -> Batch:
        baseline = Batch()
        df = pd.read_csv("{}_baseline_dataset".format(self.url))
        baseline.timestamp = df["date"][0]
        baseline.frame = df.drop(["date"])

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

    @staticmethod
    def describe_versions():
        available_versions = WeatherConfig.available_versions
        return available_versions
