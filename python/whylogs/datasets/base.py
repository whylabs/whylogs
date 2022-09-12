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
    """Batch object that encapsulate data and information for a given batch.

    - timestamp: the batch's timestamp (at the start)
    - data: the complete dataframe
    - features: input features
    - target: output feature(s)
    - prediction: output prediction and, possibly, features such as uncertainty, confidence, probability
    - misc: metadata features that are not of any of the previous categories, but still contain relevant information about the data.

    A batch can represent either a baseline or inference batch.
    The complete data is a sum of the remaining dataframe properties: features, target, prediction, and misc.
    """

    def __init__(self, timestamp: date, data: pd.DataFrame, dataset_config: DatasetConfig, version: str):
        self._timestamp = timestamp
        self._data = data
        self.dataset_config = dataset_config
        self.version = version

    @property
    def data(self) -> pd.DataFrame:
        """The complete dataframe for all available features."""

        data_cols = list(self._data.columns)
        ignore_cols = list(self.dataset_config.ignore_columns[self.version])
        return self._data[[col for col in data_cols if col not in ignore_cols]]

    @property
    def timestamp(self) -> date:
        """The batch's timestamp (at the start)"""
        return self._timestamp

    @property
    def target(self) -> pd.DataFrame:
        """Ouput feature(s)"""
        return self._data[list(self.dataset_config.target_columns[self.version])]

    @property
    def prediction(self) -> pd.DataFrame:
        """Output prediction and, possibly, features such as uncertainty, confidence, probability scores"""
        return self._data[list(self.dataset_config.prediction_columns[self.version])]

    @property
    def extra(self) -> pd.DataFrame:
        """Metadata features that are not of any of the previous categories, but still contain relevant information about the data."""
        return self._data[list(self.dataset_config.miscellaneous_columns[self.version])]

    @property
    def features(self) -> pd.DataFrame:
        """Input features"""
        data_cols = list(self._data.columns)
        target_cols = list(self.dataset_config.target_columns[self.version])
        prediction_cols = list(self.dataset_config.prediction_columns[self.version])
        misc_cols = list(self.dataset_config.miscellaneous_columns[self.version])
        ignore_cols = list(self.dataset_config.ignore_columns[self.version])
        return self._data[
            [col for col in data_cols if col not in target_cols + prediction_cols + misc_cols + ignore_cols]
        ]


class Dataset(ABC):
    """Abstract class representing a dataset."""

    @classmethod
    @abstractmethod
    def describe_versions(cls) -> List[str]:
        """Describe available versions for the given dataset."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def describe(cls) -> str:
        """Display overall dataset description."""
        raise NotImplementedError

    @abstractmethod
    def set_parameters(
        self,
        inference_interval: str,
        baseline_timestamp: Optional[Union[date, datetime]] = None,
        inference_start_timestamp: Optional[Union[date, datetime]] = None,
        original: Optional[bool] = None,
    ) -> None:
        """Set interval and timestamp parameters for the dataset object.

        Parameters
        ----------
        inference_interval : str
            Time period for each batch retrieved from the inference dataset. E.g. daily batches would be set as "1d"
        baseline_timestamp : Optional[Union[date, datetime]], optional
            The timestamp for the baseline dataset. Will be set to the dataset's original timestamp if original=True. By default None
        inference_start_timestamp : Optional[Union[date, datetime]], optional
            The timestamp for the start of the inference dataset. Will be set to the dataset's original timestamp if original=True. By default None
        original: Optional[bool], optional
            If true, will set both baseline_timestamp and inference_start_timestamp to its original values.
        """
        raise NotImplementedError

    @abstractmethod
    def get_baseline(self) -> Batch:
        """Get baseline Batch object.

        Returns
        -------
        Batch
            A batch object representing the complete baseline data.
        """
        raise NotImplementedError

    @abstractmethod
    def get_inference_data(
        self, target_date: Optional[Union[date, datetime]] = None, number_batches: Optional[int] = None
    ) -> Union[Batch, Iterable[Batch]]:
        """Get inference batch(es)

        Parameters
        ----------
        target_date : Optional[Union[date, datetime]], optional
            If target_date is set, a single batch will be returned for the given date(or datetime).
            If both target_date and number_batches are defined, an error will be raised.
        number_batches : Optional[int], optional
            If number_batches is set to `n`, an iterator of `n` inference batches will be returned, starting from `inference_start_timestamp`.
            If both target_date and number_batches are defined, an error will be raised.

        Returns
        -------
        Union[Batch, Iterable[Batch]]
            Can return either a single or multiple batches, according to the parameters passed.
        """
        raise NotImplementedError
