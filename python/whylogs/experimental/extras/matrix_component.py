from io import BytesIO

import numpy as np

from whylogs.core.metrics.deserializers import deserializer
from whylogs.core.metrics.metric_components import MetricComponent
from whylogs.core.metrics.serializers import serializer
from whylogs.core.proto import MetricComponentMessage


def _serialize_ndarray(a: np.ndarray) -> bytes:
    bio = BytesIO()
    np.save(bio, a, allow_pickle=False)
    return bio.getvalue()


def _deserialize_ndarray(a: bytes) -> np.ndarray:
    bio = BytesIO(a)
    return np.load(bio, allow_pickle=False)


class MatrixComponent(MetricComponent[np.ndarray]):
    # mtype = np.ndarray
    type_id = 101


@serializer(type_id=101)
def serialize(value: np.ndarray) -> MetricComponentMessage:
    return MetricComponentMessage(serialized_bytes=_serialize_ndarray(value))


@deserializer(type_id=101)
def deserialize(msg: MetricComponentMessage) -> np.ndarray:
    return _deserialize_ndarray(msg.serialized_bytes)
