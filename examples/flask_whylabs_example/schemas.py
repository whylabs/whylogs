from typing import Dict

from pydantic import BaseModel


class FeatureVector(BaseModel):
    sepal_length_cm: float
    sepal_width_cm: float
    petal_length_cm: float
    petal_width_cm: float
