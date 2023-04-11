import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from importlib import resources
from logging import getLogger
from typing import Iterable, Optional, Tuple, Union

import pandas as pd

from whylogs.datasets.base import Batch, Dataset
from whylogs.datasets.configs import BaseConfig, DatasetConfig, EmployeeConfig
from whylogs.datasets.utils import (
    _adjust_df_date,
    _get_dataset_path,
    _parse_interval,
    _validate_timestamp,
)

logger = getLogger(__name__)


base_config = BaseConfig()


@dataclass(init=False)
class Employee(Dataset):
    """Employee Dataset"""

    baseline_df: pd.DataFrame
    production_df: pd.DataFrame
    production_interval: str = "1d"
    number_days: int = 1
    unit: str = "D"
    url: str = EmployeeConfig.url
    baseline_timestamp: Union[date, datetime] = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    production_start_timestamp: Union[date, datetime] = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) + timedelta(days=1)
    original: bool = False
    dataset_config: Optional[DatasetConfig] = None

    @classmethod
    def config(cls) -> DatasetConfig:
        return EmployeeConfig()

    def __init__(self, version: str = "base") -> None:
        """Initializes internal dataframes.

        If the files are already present locally, won't try to download from S3.

        Parameters
        ----------
        version : str, optional
            The desired dataset's version, by default "base"

        """
        if not self.dataset_config:
            self.dataset_config = EmployeeConfig()
        if version not in self.dataset_config.available_versions:
            raise ValueError("Version not found in list of available versions.")
        self.version = version

        baseline_file = os.path.join(
            _get_dataset_path(self.dataset_config.folder_name), "baseline_dataset_{}.csv".format(self.version)
        )
        production_file = os.path.join(
            _get_dataset_path(self.dataset_config.folder_name), "production_dataset_{}.csv".format(self.version)
        )

        try:
            self.baseline_df = pd.read_csv(baseline_file)
            self.production_df = pd.read_csv(production_file)
        except FileNotFoundError:
            self.baseline_df = pd.read_csv("{}/baseline_dataset_{}.csv".format(self.url, self.version))
            self.baseline_df.to_csv(baseline_file, index=False)

            self.production_df = pd.read_csv("{}/production_dataset_{}.csv".format(self.url, self.version))
            self.production_df.to_csv(production_file, index=False)

        self.baseline_df = _adjust_df_date(self.baseline_df, new_start_date=self.baseline_timestamp)
        self.production_df = _adjust_df_date(self.production_df, new_start_date=self.production_start_timestamp)

    @classmethod
    def describe_versions(cls) -> Tuple[str]:
        available_versions = cls.config().available_versions
        return available_versions

    @classmethod
    def describe(cls) -> Optional[str]:
        descr = resources.read_text(base_config.description_folder, cls.config().description_file)
        return descr

    def get_baseline(self) -> Batch:
        data = self.baseline_df
        baseline = Batch(
            timestamp=self.baseline_timestamp, data=data, dataset_config=self.dataset_config, version=self.version
        )
        return baseline

    def _truncate_and_check_timezone(self, timestamp: datetime) -> datetime:
        if timestamp.tzinfo is None:
            logger.warning("No timezone set in the datetime_timestamp object. Default to local timezone")
            timestamp = timestamp.astimezone(tz=timezone.utc)
        timestamp = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        return timestamp

    def _validate_interval(self, interval: str) -> Tuple[int, str]:
        """Checks if desired interval are of acceptable units and inside maximum duration limits."""
        number_days, unit = _parse_interval(interval)
        if self.dataset_config is None:
            raise ValueError("default_config is unset for this dataset")
        config: EmployeeConfig = self.dataset_config
        if number_days > config.max_interval:
            raise ValueError("Maximum allowed interval for this dataset is {}".format(config.max_interval))
        if unit != "D":
            raise ValueError("Current accepted unit for this dataset is {}".format(config.base_unit))
        return (number_days, unit)

    def get_inference_data(
        self, target_date: Optional[Union[date, datetime]] = None, number_batches: Optional[int] = None
    ) -> Union[Batch, Iterable[Batch]]:
        raise NotImplementedError("Please use get_production_data instead.")

    def get_production_data(
        self, target_date: Optional[Union[date, datetime]] = None, number_batches: Optional[int] = None
    ) -> Union[Batch, Iterable[Batch]]:
        """Get batch(es) from production dataset.

        Parameters
        ----------
        target_date : Optional[Union[date, datetime]], optional
            Target date for single batch. If datetime is passed, only date will be considered, by default None
        number_batches : Optional[int], optional
            Number of batches to be retrieved. Each batch will have a time interval as defined by `production_interval` from `set_parameters`. By default None

        Returns
        -------
        Union[Batch, Iterable[Batch]]
            Can return a single batch or an interator of batches, depending on input parameters
        """
        if not target_date and not number_batches:
            raise ValueError("date or number_batches must be passed to get_production_data.")
        if target_date and number_batches:
            raise ValueError("Either date or number_batches should be passed, not both.")
        if target_date and isinstance(target_date, (date, datetime)):
            _date: datetime = _validate_timestamp(target_date)
            _date = self._truncate_and_check_timezone(_date)
            mask = self.production_df["date"] == _date
            data = self.production_df.loc[mask]
            production = Batch(timestamp=_date, data=data, dataset_config=self.dataset_config, version=self.version)
            return production
        if number_batches:
            batches = EmployeeDatasetIterator(
                self.production_df,
                number_days=self.number_days,
                number_batches=number_batches,
                version=self.version,
                config=self.dataset_config,
            )
            return batches
        raise ValueError("Target date should be either of date or datetime type.")

    def set_parameters(
        self,
        production_interval: Optional[str] = None,
        baseline_timestamp: Optional[Union[date, datetime]] = None,
        production_start_timestamp: Optional[Union[date, datetime]] = None,
        original: Optional[bool] = None,
    ) -> None:
        """Set timestamp and interval parameters for the dataset object.

        Parameters
        ----------
        production_interval : Optional[str], optional
            Interval for the production batches. If none is passed, daily production batches will be returned, by default None
        baseline_timestamp : Optional[Union[date, datetime]], optional
            Timestamp for the baseline dataset. If none is passed, timestamp will be equal to the current day, by default None
        production_start_timestamp : Optional[Union[date, datetime]], optional
            Timestamp for the start of the inference dataset. If none is passed, timestamp will be equal to tomorrow's date, by default None
        original : Optional[bool], optional
            _If true, sets both baseline and inference timestamps to the dataset's original timestamp, by default None
        """
        if production_interval:
            self.production_interval = production_interval
            self.number_days, self.unit = self._validate_interval(self.production_interval)
        if original:
            assert self.dataset_config is not None
            config = self.dataset_config
            self.baseline_timestamp = config.baseline_start_timestamp[self.version]
            self.production_start_timestamp = config.production_start_timestamp[self.version]
            self.production_df = _adjust_df_date(self.production_df, self.production_start_timestamp)
            self.baseline_df = _adjust_df_date(self.baseline_df, new_start_date=self.baseline_timestamp)

        if baseline_timestamp:
            if not original:
                _baseline_date: datetime = _validate_timestamp(baseline_timestamp)
                _baseline_date = self._truncate_and_check_timezone(_baseline_date)
                self.baseline_timestamp = _baseline_date
                self.baseline_df = _adjust_df_date(self.baseline_df, self.baseline_timestamp)

            else:
                logger.warning(
                    "baseline_timestamp and production_start_timestamp overriden by original timestamps due to original = True"
                )

        if production_start_timestamp:
            if not original:
                _production_date: datetime = _validate_timestamp(production_start_timestamp)
                _production_date = self._truncate_and_check_timezone(_production_date)
                self.production_start_timestamp = _production_date
                self.production_df = _adjust_df_date(self.production_df, self.production_start_timestamp)
            else:
                logger.warning(
                    "baseline_timestamp and production_start_timestamp overriden by original timestamps due to original = True"
                )


class EmployeeDatasetIterator:
    """Iterator to retrieve production batches, when multiple batches are required."""

    def __init__(
        self, df: pd.DataFrame, number_days: int, number_batches: int, version: str, config=DatasetConfig
    ) -> None:
        self._df = df
        self._number_days = number_days
        self._number_batches = number_batches
        self.version = version
        self.config = config

    def __iter__(self):
        self._index: date = self._df.iloc[0].name
        self._batch_counter: int = 0
        return self

    def __next__(self) -> Batch:
        if self._batch_counter >= self._number_batches:
            raise StopIteration
        if self._index <= self._df.iloc[-1].name:
            day = self._index
            data = self._df[day : day + timedelta(days=(self._number_days - 1))]  # type: ignore
            production = Batch(timestamp=day, data=data, dataset_config=self.config, version=self.version)
            self._index = data.index[-1] + timedelta(days=1)
            self._batch_counter += 1

            return production

        raise StopIteration
