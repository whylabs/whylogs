import hashlib
import inspect
import logging
from dataclasses import dataclass, field
from typing import Callable, List, Mapping, Optional

from whylogs.api.usage_stats import emit_usage
from whylogs.core.projectors import FieldProjector

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
        filter_string = self.filter_id
        mapper_string = self.mapper.id if self.mapper else ""
        segment_hash = hashlib.sha512(bytes(filter_string + mapper_string, encoding="utf8"))
        self.id = segment_hash.hexdigest()

    @property
    def simple(self) -> bool:
        return self.mapper is None or self.mapper.map is None

    @property
    def filter_id(self) -> str:
        if self.filter:
            if self.filter.filter_function:
                return inspect.getsource(self.filter.filter_function)
            if self.filter.query_string:
                return self.filter.query_string
        return ""

    def __eq__(self, other) -> bool:
        if isinstance(other, SegmentationPartition):
            return self.id == other.id
        return False

    def __hash__(self):
        return hash(self.id)


def segment_on_column(column_name: str) -> Mapping[str, SegmentationPartition]:
    emit_usage("segment_on_column")

    return {column_name: SegmentationPartition(name=column_name, mapper=ColumnMapperFunction(col_names=[column_name]))}
