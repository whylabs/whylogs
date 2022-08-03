import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, List, Optional, Union

import pandas as pd

from whylogs.datasets.configs import DatasetConfig

logger = logging.getLogger(__name__)


@dataclass(init=False)
class Batch:
    """Batch object.
    data = features + target + prediction + meta
    """

    def __init__(self, timestamp: date, data: pd.DataFrame, dataset_config: DatasetConfig, version: str):
        self._timestamp = timestamp
        self._data = data
        self.dataset_config = dataset_config
        self.version = version

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @property
    def timestamp(self) -> date:
        return self._timestamp

    @property
    def target(self) -> pd.DataFrame:
        return self._data[list(self.dataset_config.target_columns[self.version])]

    @property
    def prediction(self) -> pd.DataFrame:
        return self._data[list(self.dataset_config.prediction_columns[self.version])]

    @property
    def meta(self) -> pd.DataFrame:
        return self._data[list(self.dataset_config.metadata_columns[self.version])]

    @property
    def features(self) -> pd.DataFrame:
        data_cols = list(self._data.columns)
        target_cols = list(self.dataset_config.target_columns[self.version])
        prediction_cols = list(self.dataset_config.prediction_columns[self.version])
        meta_cols = list(self.dataset_config.metadata_columns[self.version])
        return self._data[[col for col in data_cols if col not in target_cols + prediction_cols + meta_cols]]


class Dataset(ABC):
    @classmethod
    @abstractmethod
    def describe_versions(cls) -> List[str]:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def describe(cls) -> str:
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

    @abstractmethod
    def get_inference_data(
        self, target_date: Optional[Union[date, datetime]] = None, number_batches: Optional[int] = None
    ) -> Union[Batch, Iterable[Batch]]:
        raise NotImplementedError
