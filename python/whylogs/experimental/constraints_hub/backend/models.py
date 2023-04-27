from typing import Dict, List

from pydantic import BaseModel


class ConstraintColumn(BaseModel):
    type: str
    checks: List[Dict]
    metrics: List[str]


class Constraints(BaseModel):
    constraint_list: List[ConstraintColumn]


class EntitySchema(BaseModel):
    columns: Dict[str, Dict]
