from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Union


@dataclass
class DateQuery:
    dataset_id: str
    start_date: datetime
    end_date: Optional[datetime] = field(default=None)
    segment_tag: Optional[str] = field(default=None)

    def __post_init__(self):
        if self.end_date is None:
            self.end_date = self.start_date
        if self.end_date < self.start_date:
            self.start_date, self.end_date = self.end_date, self.start_date


@dataclass
class DatasetIdQuery:
    dataset_id: str
    segment_tag: Optional[str] = field(default=None)


BaseQuery = Union[DateQuery, DatasetIdQuery]
