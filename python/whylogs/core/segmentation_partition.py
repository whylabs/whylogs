import hashlib
import inspect
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from whylogs.core.projectors import FieldProjector
from whylogs.core.segment import Segment
from whylogs.core.stubs import pd

logger = logging.getLogger(__name__)

MAX_SEGMENTS_ERROR_THRESHOLD = 2000  # if profiles are up to 500KiB each, then 500KiB * 2000 = 1 GiB
MAX_SEGMENTS_WARNING_THRESHOLD = 200


@dataclass(frozen=False)
class ColumnMapperFunction:
    col_names: Optional[List[str]] = None
    map: Optional[Callable] = None
    field_projector: FieldProjector = field(init=False)
    id: str = field(init=False)

    def __post_init__(self):
        self.field_projector = FieldProjector(col_names=self.col_names)
        mapper_string = inspect.getsource(self.map) if self.map else ""
        column_string = ",".join(sorted(self.col_names))
        segment_hash = hashlib.sha512(bytes(column_string + mapper_string, encoding="utf8"))
        self.id = segment_hash.hexdigest()

    def apply(self, pandas: Optional[pd.DataFrame], row: Optional[Dict[str, Any]]) -> Tuple[str]:
        if not self.col_names and not self.map:
            raise ValueError("Column mapping must specify at least one column name or define a map function")
        result = tuple()
        if self.map:
            result = self.map(row)
        else:
            result = tuple(column_name + "[" + str(row.get(column_name)) + "]" for column_name in self.col_names)
        return result

    def __call__(self, row: Dict[str, Any]) -> Tuple[str]:
        return self.apply(self.field_projector(row))


@dataclass
class SegmentFilter:
    filter_function: Optional[Callable] = None
    query_string: Optional[str] = None


@dataclass(frozen=False)
class SegmentationPartition:
    name: str
    mapper: Optional[ColumnMapperFunction] = None
    id: str = field(init=False)
    filter: Optional[SegmentFilter] = None

    def __post_init__(self):
        filter_string = inspect.getsource(self.filter) if self.filter else ""
        mapper_string = self.mapper.id if self.mapper else ""
        segment_hash = hashlib.sha512(bytes(filter_string + mapper_string, encoding="utf8"))
        self.id = segment_hash.hexdigest()

    @property
    def simple(self) -> bool:
        return self.mapper is None or self.mapper.map is None


def segment_on_column(column_name: str) -> SegmentationPartition:
    return SegmentationPartition(name=column_name, mapper=ColumnMapperFunction(col_names=[column_name]))
