from abc import ABC
from datetime import datetime
from typing import Optional


class BaseQuery(ABC):
    def __init__(self):
        self.dataset_id = None


class DateQuery(BaseQuery):
    def __init__(
        self,
        dataset_id: str,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        segment_tag: Optional[str] = None,
    ):

        super().__init__()
        self.dataset_id = dataset_id
        self.start_date = start_date
        self.end_date = end_date
        self.segment_tag = segment_tag

        if self.end_date is None:
            self.end_date = self.start_date
        if self.end_date < self.start_date:
            self.start_date, self.end_date = self.end_date, self.start_date


class DatasetIdQuery(BaseQuery):
    def __init__(self, dataset_id: str, segment_tag: Optional[str] = None):
        super().__init__()
        self.dataset_id = dataset_id
        self.segment_tag = segment_tag
