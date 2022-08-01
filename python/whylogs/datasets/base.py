from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Union, Iterable, Tuple

from datetime import date, datetime
import logging
from xml.sax.handler import feature_namespaces
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class Batch:
    """Batch object.
    data = features + target + prediction + meta
    """

    timestamp: date
    data: pd.DataFrame
    features: Optional[pd.DataFrame] = None
    target: Optional[pd.DataFrame] = None
    prediction: Optional[pd.DataFrame] = None
    meta: Optional[pd.DataFrame] = None  # maybe change name
    DESCR: Optional[str] = None


class Dataset(ABC):
    @staticmethod
    @abstractmethod
    def describe_versions() -> "list[str]":
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
