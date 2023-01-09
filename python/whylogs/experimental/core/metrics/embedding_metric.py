from dataclasses import dataclass
from enum import Enum
from io import BytesIO
from typing import List, Optional

import numpy as np
from sklearn.metrics.pairwise import cosine_distances, euclidean_distances

from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.deserializers import deserializer
from whylogs.core.metrics.metric_components import MetricComponent
from whylogs.core.metrics.metrics import MetricConfig, OperationResult
from whylogs.core.metrics.multimetric import MultiMetric
from whylogs.core.metrics.serializers import serializer
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricComponentMessage

# TODO: share these with NLP code


def _serialize_ndarray(a: np.ndarray) -> bytes:
    bio = BytesIO()
    np.save(bio, a, allow_pickle=False)
    return bio.getvalue()


def _deserialize_ndarray(a: bytes) -> np.ndarray:
    bio = BytesIO(a)
    return np.load(bio, allow_pickle=False)


class MatrixComponent(MetricComponent[np.ndarray]):
    #    mtype = np.ndarray
    type_id = 101


@serializer(type_id=101)
def serialize(value: np.ndarray) -> MetricComponentMessage:
    return MetricComponentMessage(serialized_bytes=_serialize_ndarray(value))


@deserializer(type_id=101)
def deserialize(msg: MetricComponentMessage) -> np.ndarray:
    return _deserialize_ndarray(msg.serialized_bytes)


class DistanceFunction(Enum):
    euclidean = euclidean_distances
    cosine = cosine_distances


@dataclass(frozen=True)
class EmbeddingConfig(MetricConfig):
    references: np.ndarray  # columns are reference vectors
    labels: Optional[List[str]] = None
    distance_fn: DistanceFunction = DistanceFunction.cosine

    def __post_init__(self) -> None:
        if len(self.references.shape) != 2:
            raise ValueError("Embedding reference matrix must be 2 dimensional")

        if self.labels:
            if len(self.labels) == self.references.shape[1]:
                raise ValueError(
                    f"Number of labels ({len(self.labels)}) must match number of reference vectors ({self.references.shape[1]})"
                )


@dataclass
class EmbeddingMetric(MultiMetric):
    references: MatrixComponent
    labels: List[str]
    distance_fn: DistanceFunction

    def __post_init__(self):
        submetrics = {
            f"{label}_distance": {
                "distribution": StandardMetric.distribution.zero(),
                "counts": StandardMetric.counts.zero(),
                "types": StandardMetric.types.zero(),
            }
            for label in self.labels
        }
        submetrics.update(
            {
                "closest": {
                    "frequent_items": StandardMetric.frequent_items.zero(),
                    "counts": StandardMetric.counts.zero(),
                    "types": StandardMetric.types.zero(),
                }
            }
        )
        super().__init__(submetrics)

    @property
    def namespace(self) -> str:
        return "embedding"

    def _update_submetrics(self, submetric: str, data: PreprocessedColumn) -> None:
        for key in self.submetrics[submetric].keys():
            self.submetrics[submetric][key].columnar_update(data)

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        X = data.list.objs  # TODO: throw if not 2D
        if not X:
            return OperationResult.ok(0)

        X_ref_dists = self.distance_fn.value(X, self.references.value)
        X_ref_closest = np.argmin(self.X_ref_dists, axis=1)
        closest: List[str] = []
        for i in range(X_ref_dists.shape[1]):
            closest.append(self.lables[X_ref_closest[i]])
            self._update_submetrics(f"{i}_distance", PreprocessedColumn.apply(X_ref_dists[i]))

        self._update_submetrics("closest", PreprocessedColumn.apply(closest))

        return OperationResult.ok(1)

    @classmethod
    def zero(cls, cfg: Optional[EmbeddingConfig] = None) -> "EmbeddingMetric":
        cfg = cfg or EmbeddingConfig(np.zeros((1, 1)))
        if not isinstance(cfg, EmbeddingConfig):
            raise ValueError("EmbeddingMetric.zero() requires EmbeddingConfig argument")

        return EmbeddingMetric(
            MatrixComponent(cfg.references),
            cfg.labels or [str(i) for i in range(cfg.references.shape[1])],
            cfg.distance_fn,
        )
