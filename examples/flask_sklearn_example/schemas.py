from typing import List
from pydantic import BaseModel


class FeatureVector(BaseModel):
    data: List[float]
