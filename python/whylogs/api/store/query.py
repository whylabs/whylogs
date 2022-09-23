from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class DateQuery:
    start_date: datetime
    end_date: Optional[datetime] = field(default=None)

    def __post_init__(self):
        if self.end_date is None:
            self.end_date = self.start_date
        if self.end_date < self.start_date:
            self.start_date, self.end_date = self.end_date, self.start_date


# TODO allow for more or less granular queries -> hourly, yearly
