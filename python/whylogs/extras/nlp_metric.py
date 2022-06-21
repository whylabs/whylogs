from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, Optional, Tuple

import numpy as np

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics.compound_metric import CompoundMetric
from whylogs.core.metrics.metric_components import (
    FractionalComponent,
    IntegralComponent,
    MetricComponent,
    Registries,
)
from whylogs.core.metrics.metrics import (
    DistributionMetric,
    FrequentItemsMetric,
    Metric,
    MetricConfig,
    OperationResult,
)
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricComponentMessage, MetricMessage

_SMALL = np.finfo(float).eps


def _reciprocal(s: np.ndarray) -> np.ndarray:
    """Return pseudoinverse of singular value vector"""
    # should also zap if too small relative to s[0]
    return np.array([1 / x if x > _SMALL else 0 for x in s])


def _serialize_ndarray(a: np.ndarray) -> bytes:
    bio = BytesIO()
    np.save(bio, a, allow_pickle=False)
    return bio.getvalue()


def _deserialize_ndarray(a: bytes) -> np.ndarray:
    bio = BytesIO(a)
    return np.load(bio, allow_pickle=False)


class VectorComponent(MetricComponent[np.ndarray]):
    mtype = np.ndarray
    index = 101

    def to_protobuf(self) -> MetricComponentMessage:
        return MetricComponentMessage(serialized_bytes=_serialize_ndarray(self.value))

    @classmethod
    def from_protobuf(cls, msg: MetricComponentMessage, registries: Optional[Registries] = None) -> "VectorComponent":
        return VectorComponent(value=_deserialize_ndarray(msg.serialized_bytes))


@dataclass(frozen=True)
class SvdMetricConfig(MetricConfig):
    k: int
    decay: float = 1.0


# The SvdMetric classes just hold (and optionally update) the SVD
# sketch. They can compute residuals from the current SVD approximation,
# but they do not maintain any statistics about the distribution. See
# the NlpMetric for tracking the residual distribution.

@dataclass(frozen=True)
class SvdMetric(Metric):
    """
    non-updating SVD metric
    """

    k: IntegralComponent  # SVD truncation  k > 0
    decay: FractionalComponent  # 0 < decay <= 1  decay rate of old data
    U: VectorComponent  # left singular vectors
    S: VectorComponent  # singular values

    @property
    def namespace(self) -> str:
        return "svd"
            
    def residual(self, vector: np.ndarray) -> float:
        """
        Retruns the residual of the vector given the current approximate SVD:
        residual = || U S S^{+} U' x - x || / || x ||  where x is the vector
        """
        U = self.U.value
        S = self.S.value
        residual = U.transpose * vector
        residual = _reciprocal(S) * residual
        residual = S * residual
        residual = U * residual
        residual = residual - vector
        residual = np.linalg.norm(residual) / np.linalg.norm(vector)
        return residual

    def merge(self, other: "SvdMetric") -> "SvdMetric":
        # non-updating!
        return SvdMetric(self.k, self.decay, self.U, self.S)

    def to_summary_dict(self, cfg: SummaryConfig) -> Dict[str, Any]:
        # this will be large and probably not interesting
        return {
            "k": self.k.value,
            "decay": self.decay.value,
            "U": self.U.value,
            "S": self.S.value,
        }

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        # non-updating!
        return OperationResult.ok(1)

    @classmethod
    def zero(cls, config: MetricConfig) -> "SvdMetric":
        """
        Instances created with zero() will be useless because they're
        not updatable.
        """
        if config is None or not isinstance(config, SvdMetricConfig):
            raise ValueError("SvdMetric.zero() requires SvdMetricConfig argument")

         return SvdMetric(
            k=IntegralComponent(0),  # not usable with k = 0
            decay=FractionalComponent(1.0),
            U=VectorComponent(np.zeros((1, 1))),
            S=VectorComponent(np.zeros(1)),
        )


@dataclass(frozen=True)
class UpdatableSvdMetric(SvdMetric):
    """
    updating SVD metric
    """

    @property
    def namespace(self) -> str:
        return "updatable_svd"

    def _resketch(self, k: int, decay: float, U1: np.ndarray, S1: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        U0, S0 = self.U.value, self.S.value
        if U0.shape == (0, 0):
            U0 = np.zeros((U1.shape[0], k))
            S0 = np.zeros(k)
        if U0.shape[0] < U1.shape[0]:
            U0 = np.pad(U0, ((0, U1.shape[0] - U0.shape[0]), (0, 0)), "constant")
        assert U0.shape[0] == U1.shape[0]

        Q, R = np.linalg.qr(np.concatenate((decay * U0 * S0, U1 * S1), axis=1))
        UR, S, VRT = np.linalg.svd(R)
        U = np.dot(Q, UR)
        return U, S

    def merge(self, other: "SvdMetric") -> "UpdatableSvdMetric":
        # other can be updatable or not
        new_U, new_S = self._resketch(self.k.value, self.decay.value, other.U.value, other.S.value)
        return UpdatableSvdMetric(self.k, self.decay, VectorComponent(new_U), VectorComponent(new_S))

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        k = self.k.value
        decay = self.decay.value
        vectors_processed = 0
        for vector in data.list.objs:
            if (not isinstance(vector, np.ndarray)) or vector.shape[0] < 2:
                continue

            # TODO: batch this
            vectors_processed += 1
            U1, S1, _ = np.linalg.svd(vector.reshape((vector.shape[0], 1)), False, True, False)
            new_U, new_S = self._resketch(k, decay, U1, S1)
            self.U.set(new_U)
            self.S.set(new_S)

        return OperationResult.ok(vectors_processed)

    @classmethod
    def zero(cls, config: SvdMetricConfig) -> "UpdatableSvdMetric":
        if config is None or not isinstance(config, SvdMetricConfig):
            raise ValueError("UpdatableSvdMetric.zero() requires SvdMetricConfig argument")

        return UpdatableSvdMetric(
            k=IntegralComponent(config.k),  # not usable with k = 0
            decay=FractionalComponent(config.decay),
            U=VectorComponent(np.zeros((1, 1))),
            S=VectorComponent(np.zeros(1)),
        )


@dataclass(frozen=True)
class NlpConfig(MetricConfig):
    """
    If you pass in an UpdatableSvdMetric, the SVD will be updated along with the
    NlpMetric's residual distribution. A non-updatable SvdMetric will update the
    residual distribution, but it will not update the SVD as new term vectors are
    processed.

    Note that the [Updatable]SvdMetric is not [de]serialized with the NlpMetric.
    You'll have to manage that yourself.
    """

    svd: SvdMetric


class NlpMetric(CompoundMetric):
    """
    Natural language processing metric
    """

    svd: SvdMetric  # use an UpdatableSvdMetric to train while tracking, or SvdMetric if SVD is to be static

    def __post_init__(self):
        submetrics = {
            "doc_length": DistributionMetric.zero(ColumnSchema(dtype=int)),
            "term_length": DistributionMetric.zero(ColumnSchema(dtype=int)),
            "residual": DistributionMetric.zero(ColumnSchema(dtype=float)),
            "frequent_terms": FrequentItemsMetric.zero(ColumnSchema(dtype=str)),
        }
        super(NlpMetric, self).__post_init__()

    @property
    def namespace(self) -> str:
        return "nlp"

    def merge(self, other: "NlpMetric") -> "NlpMetric":
        result = super(NlpMetric, self).merge(other)  # update all of our submetrics
        result.svd = self.svd.merge(other.svd)  # update if self.svd is updatable, else no-op
        return result

    # CompoundMetric {to,from}_protobuf(), to_summary_dict() -- you have to serialize NlpMetric.svd yourself if it updated

    # data.list.strings is the list of strings in a (single) document
    # data.list.objs is as list of np.ndarray. Each ndarray represents one document's term vector.

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        terms = data.list.strings
        if terms:
            term_lengths = [len(term) for term in terms]
            self.submetrics["term_length"].columnar_update(PreprocessedColumn.apply(term_lengths))
            self.submetrics["frequent_terms"].columnar_update(PreprecessedColumn.apply(terms))
            self.submetrics["doc_length"].columnar_update(PreprocessedColumn.apply([len(terms)]))

        self.svd.columnar_update(data)  # no-op if SVD is not updating
        residuals: List[float] = []
        for vector in data.list.objs
            residuals += self.svd.residual(vector)

        self.submetrics["residual"].columnar_update(PreprocessedColumn.apply(residuals))
        return OperationResult.ok(1)

    @classmethod
    def zero(cls, config: NlpConfig) -> "NlpMetric":
        return NlpMetric(config.svd)  # k, decay?

    @classmethod
    def from_protobuf(cls, msg: MetricMessage) -> "NlpMetric":
        submetrics = cls.submetrics_from_protobuf(msg)
        result = NlpMetric(SvdMetric.zero(SvdMetricConfig(0, 1.0)))  # not updatable, can't compute residuals
        result.submetrics = submetrics
        return result


def _preprocessifier(terms: List[str], vector: np.ndarray) -> PreproccessedColumn:
    strings = terms
    objs = [vector]
    list_view = ListView(strings=strings, objs=objs)
    result = PreprocessedColumn()
    result.list = list_view
