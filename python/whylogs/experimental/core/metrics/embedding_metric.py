import logging
from dataclasses import dataclass, field
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
from whylogs.core.proto import MetricComponentMessage, MetricMessage

logger = logging.getLogger(__name__)


# TODO: share these with NLP code


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


class DistanceFunction(Enum):
    euclidean = euclidean_distances
    cosine = cosine_distances


@dataclass(frozen=True)
class EmbeddingConfig(MetricConfig):
    references: np.ndarray = field(default_factory=lambda: np.zeros((1, 1)))  # rows are reference vectors
    labels: Optional[List[str]] = None
    distance_fn: DistanceFunction = DistanceFunction.cosine
    serialize_references: bool = True

    # TODO: limit refeence size

    def __post_init__(self) -> None:
        if len(self.references.shape) != 2:
            raise ValueError("Embedding reference matrix must be 2 dimensional")

        if self.labels:
            if len(self.labels) != self.references.shape[0]:
                raise ValueError(
                    f"Number of labels ({len(self.labels)}) must match number of reference vectors ({self.references.shape[1]})"
                )


@dataclass
class EmbeddingMetric(MultiMetric):
    references: MatrixComponent
    labels: List[str]
    distance_fn: DistanceFunction
    serialize_references: bool

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

    def merge(self, other: "EmbeddingMetric") -> "EmbeddingMetric":
        if self.references.value.shape != other.references.value.shape:
            if other.references.value.shape == (1, 1):
                logger.warning("Attempt to merge with unconfigured EmbeddingMetric; ignored")
                return self
            if self.references.value.shape == (1, 1):
                logger.warning("Attempt to merge with unconfigured EmbeddingMetric; ignored")
                return other
            raise ValueError("Attempt to merge incompatible EbeddingMetrics")

        if (
            self.labels != other.labels
            or self.distance_fn != other.distance_fn
            or not (self.references.value == other.references.value).all()
        ):
            raise ValueError("Attempt to merge incompatible EbeddingMetrics")

        result = EmbeddingMetric(self.references, self.labels, self.distance_fn, self.serialize_references)
        result.submetrics = self.merge_submetrics(other)
        return result

    def to_protobuf(self) -> MetricMessage:
        msg = {}
        for sub_name, metrics in self.submetrics.items():
            for namespace, metric in metrics.items():
                sub_msg = metric.to_protobuf()
                for comp_name, comp_msg in sub_msg.metric_components.items():
                    msg[f"{sub_name}:{namespace}/{comp_name}"] = comp_msg
        if self.serialize_references:
            msg["references"] = self.references.to_protobuf()

        return MetricMessage(metric_components=msg)

    def _update_submetrics(self, submetric: str, data: PreprocessedColumn) -> None:
        for key in self.submetrics[submetric].keys():
            self.submetrics[submetric][key].columnar_update(data)

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        if data.numpy.ints is None and data.numpy.floats is None:
            return OperationResult.ok(0)

        matrices = [X for X in [data.numpy.ints, data.numpy.floats] if X is not None]
        for X in matrices:  # TODO: throw if not 2D ndarray
            X_ref_dists = self.distance_fn(X, self.references.value)  # type: ignore
            X_ref_closest = np.argmin(X_ref_dists, axis=1)
            print(f"ref dists: {X_ref_dists}")
            print(f"closest: {X_ref_closest}")
            for i in range(X_ref_dists.shape[1]):
                print(f"updating {self.labels[i]}_distance with {X_ref_dists[ :, i].tolist()}")
                self._update_submetrics(
                    f"{self.labels[i]}_distance", PreprocessedColumn.apply(X_ref_dists[:, i].tolist())
                )

            closest = [self.labels[i] for i in X_ref_closest]
            print(f"updating closest with {closest}")
            self._update_submetrics("closest", PreprocessedColumn.apply(closest))

        return OperationResult.ok(len(matrices))

    @classmethod
    def from_protobuf(cls, msg: MetricMessage) -> "EmbeddingMetric":
        if "references" in msg.metric_components:
            references = MatrixComponent.from_protobuf(msg.metric_components["references"])
            msg.metric_components.pop("references")
            serialize_references = True
        else:
            references = np.zeros((1, 1))
            serialize_references = False

        submetrics = EmbeddingMetric.submetrics_from_protobuf(msg)

        # TODO: this doesn't guarantee order :(  Fix it, or don't [de]serialize
        labels: List[str] = []
        for submetric_name in submetrics.keys():
            if submetric_name.endswith("_distance"):
                labels.append(submetric_name[:-9])

        result = EmbeddingMetric(
            references=references,
            labels=labels,
            distance_fn=DistanceFunction.cosine,  # not updatable after deserialization
            serialize_references=serialize_references,
        )
        result.submetrics = submetrics
        return result

    @classmethod
    def zero(cls, cfg: Optional[EmbeddingConfig] = None) -> "EmbeddingMetric":
        cfg = cfg or EmbeddingConfig()
        if not isinstance(cfg, EmbeddingConfig):
            raise ValueError("EmbeddingMetric.zero() requires EmbeddingConfig argument")

        return EmbeddingMetric(
            references=MatrixComponent(cfg.references),
            labels=cfg.labels or [str(i) for i in range(cfg.references.shape[0])],
            distance_fn=cfg.distance_fn,
            serialize_references=cfg.serialize_references,
        )
