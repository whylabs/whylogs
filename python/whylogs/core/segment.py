from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class Segment:
    key: Tuple[str, ...]
    parent_id: str
