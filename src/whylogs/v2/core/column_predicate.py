from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Type

"""
Predicate for columns to be used with trackers to describe configuration of profiles, so that
a tracker can filter which columns or types of data they should track.

Attributes:
    target_column_name (str): Description
    column_type (Optional[Type]): Description
"""


@dataclass
class ColumnPredicate(ABC):
    target_column_name: str = ""
    column_type: Optional[Type] = None

    def __call__(self, column_value) -> bool:
        return self.evaluate(column_value)

    @abstractmethod
    def evaluate(self, column_value) -> bool:
        pass
