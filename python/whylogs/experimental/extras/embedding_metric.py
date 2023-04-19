import logging
from dataclasses import dataclass, field
from enum import Enum
from itertools import chain
from typing import List, Optional

from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.metrics import MetricConfig, OperationResult, register_metric
from whylogs.core.metrics.multimetric import MultiMetric
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricMessage
from whylogs.core.stubs import np, sklp
from whylogs.experimental.extras.matrix_component import MatrixComponent

logger = logging.getLogger(__name__)


class DistanceFunction(Enum):
    euclidean = sklp.euclidean_distances
    cosine = sklp.cosine_distances


@dataclass(frozen=True)
class EmbeddingConfig(MetricConfig):
    """
    The rows of references are the reference vectors. A shape of (1, 1) indicates
    there's no reference matrix and the metric will not be updatable. It should still
    be mergeable with compatible metrics (details of compatability TBD). The rows
    must be in the same order as labels. If labels are not provided, "0" ... "n" will
    be used, where n is the number of rows in the references matrix.
    """

    references: np.ndarray = field(default_factory=lambda: np.zeros((1, 1)))
    labels: Optional[List[str]] = None
    distance_fn: DistanceFunction = DistanceFunction.cosine
    serialize_references: bool = True  # should references be included in protobuf message?

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
        """
        Each label has a {label}_distance submetric that tracks the distribution of
        distances from the label's reference vector to every logged vector.

        There is also a closest submetric that tracks how often a label's reference
        vector is the closest to the logged vectors.
        """

        submetrics = {
            f"{label}_distance": {
                "distribution": StandardMetric.distribution.zero(),
                "counts": StandardMetric.counts.zero(),
                "types": StandardMetric.types.zero(),
                "cardinality": StandardMetric.cardinality.zero(),
            }
            for label in self.labels
        }
        submetrics.update(
            {
                "closest": {
                    "frequent_items": StandardMetric.frequent_items.zero(),
                    "counts": StandardMetric.counts.zero(),
                    "types": StandardMetric.types.zero(),
                    "cardinality": StandardMetric.cardinality.zero(),
                }
            }
        )
        super().__init__(submetrics)
        # sort labels and permute reference matrix rows to match to support deserialization
        label_indices = np.argsort(self.labels).tolist()
        self.labels = sorted(self.labels)
        self.references = MatrixComponent(self.references.value[label_indices, :])

    @property
    def namespace(self) -> str:
        return "embedding"

    def merge(self, other: "EmbeddingMetric") -> "EmbeddingMetric":
        if self.references.value.shape != other.references.value.shape:
            if other.references.value.shape == (1, 1):
                # TODO: handle merging with other.serialize_references==False better
                # The (1, 1) shape indicates the other metric was created without a reference matrix.
                # It can't have meaningful data in it, so just return myself
                logger.warning("Attempt to merge with unconfigured EmbeddingMetric; ignored")
                return self
            if self.references.value.shape == (1, 1):
                # See comment above
                logger.warning("Attempt to merge with unconfigured EmbeddingMetric; ignored")
                return other
            raise ValueError("Attempt to merge incompatible EbeddingMetrics")

        if (
            self.labels != other.labels
            or not (self.references.value == other.references.value).all()
            # TODO: maybe   or self.distance_fn != other.distance_fn ? warn if != ?
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
        reference_dim = self.references.value.shape[1]  # number of columns in reference matrix
        successes = 0
        failures = 0
        pandas_tensors = data.pandas.tensors if data.pandas.tensors is not None else []
        for matrix in chain(data.list.tensors or [], pandas_tensors):  # TODO: stack these
            if len(matrix.shape) == 1:
                matrix = matrix.reshape((1, matrix.shape[0]))
            if len(matrix.shape) != 2 or matrix.shape[1] != reference_dim:
                logger.warn(
                    f"EmbeddingMetric requires 1 x {reference_dim} matrices; got tensor with shape {matrix.shape}"
                )
                failures += 1
                continue

            ref_dists = self.distance_fn(matrix, self.references.value)  # type: ignore
            ref_closest = np.argmin(ref_dists, axis=1)

            for i in range(ref_dists.shape[1]):
                self._update_submetrics(f"{self.labels[i]}_distance", PreprocessedColumn.apply(ref_dists[:, i]))

            closest = [self.labels[i] for i in ref_closest]
            self._update_submetrics("closest", PreprocessedColumn.apply(np.asarray(closest)))
            successes += 1

        return OperationResult(failures, successes)

    @classmethod
    def from_protobuf(cls, msg: MetricMessage) -> "EmbeddingMetric":
        if "references" in msg.metric_components:
            references = MatrixComponent.from_protobuf(msg.metric_components["references"])
            msg.metric_components.pop("references")  # it's not a submetric's component
            serialize_references = True
        else:
            references = np.zeros((1, 1))  # indicate I don't have a usuable reference matrix
            serialize_references = False

        submetrics = EmbeddingMetric.submetrics_from_protobuf(msg)

        # figure out what my labels were from the {label}_distance submetric names
        labels: List[str] = []
        for submetric_name in submetrics.keys():
            if submetric_name.endswith("_distance"):
                labels.append(submetric_name[:-9])
        labels = sorted(labels)  # the rows should already be in this order

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


# Register it so Multimetric and ProfileView can deserialize
register_metric(EmbeddingMetric)
