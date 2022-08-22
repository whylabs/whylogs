from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from whylogs.api.reader import Reader, Readers
from whylogs.api.writer import Writer, Writers
from whylogs.api.writer.writer import Writable
from whylogs.core import DatasetProfile, DatasetProfileView
from whylogs.core.schema import DatasetSchema
from whylogs.core.segmentation_partition import Segment, SegmentationPartition
from whylogs.core.stubs import pd


class ProfileStore(ABC):
    """
    A holder object for profiles.
    """

    @abstractmethod
    def get_matching_profile(
        self,
        obj: Any = None,
        *,
        pandas: Optional[pd.DataFrame] = None,
        row: Optional[Dict[str, Any]] = None,
        segment: Optional[Segment] = None,
        schema: Optional[DatasetSchema] = None,
    ) -> List[DatasetProfile]:
        return [DatasetProfile(schema)]

    @abstractmethod
    def read(
        self,
        id: Optional[str],
        dataset_timestamp: Optional[datetime] = None,
        segment: Optional[Segment] = None,
    ) -> Optional[DatasetProfile]:
        return [DatasetProfile(schema)]

    @abstractmethod
    def write(
        self,
        data: Writable,
        model_id: Optional[str] = None,
        dataset_timestamp: Optional[datetime] = None,
        segment: Optional[Segment] = None,
        schema: Optional[DatasetSchema] = None,
    ) -> None:
        pass

    @abstractmethod
    def query(self, *args: Any, **kwargs: Any) -> List[DatasetProfile]:
        pass


class SegmentedProfileStore(ProfileStore):
    pass
