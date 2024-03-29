from pydantic import BaseModel  # type: ignore


class FeatureVector(BaseModel):
    sepal_length_cm: float
    sepal_width_cm: float
    petal_length_cm: float
    petal_width_cm: float
