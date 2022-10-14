from copy import deepcopy
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import scipy as sp

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.multimetric import MultiMetric
from whylogs.core.metrics.deserializers import deserializer
from whylogs.core.metrics.metric_components import (
    FractionalComponent,
    IntegralComponent,
    MetricComponent,
)
from whylogs.core.metrics.metrics import (
    DistributionMetric,
    FrequentItemsMetric,
    Metric,
    MetricConfig,
    OperationResult,
)
from whylogs.core.metrics.serializers import serializer
from whylogs.core.preprocessing import ListView, PreprocessedColumn
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
    #    mtype = np.ndarray
    type_id = 101


@serializer(type_id=101)
def serialize(value: np.ndarray) -> MetricComponentMessage:
    return MetricComponentMessage(serialized_bytes=_serialize_ndarray(value))


@deserializer(type_id=101)
def deserialize(msg: MetricComponentMessage) -> np.ndarray:
    return _deserialize_ndarray(msg.serialized_bytes)


"""
    def to_protobuf(self) -> MetricComponentMessage:
        return MetricComponentMessage(type_id=self.type_id, serialized_bytes=_serialize_ndarray(self.value))

    @classmethod
    def from_protobuf(cls, msg: MetricComponentMessage, registries: Optional[Registries] = None) -> "VectorComponent":
        return VectorComponent(value=_deserialize_ndarray(msg.serialized_bytes))
"""


@dataclass(frozen=True)
class SvdMetricConfig(MetricConfig):
    k: int = 100
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
        # TODO: zero-pad vector if it's too short; complain if it's too long
        U = self.U.value
        S = self.S.value
        residual = U.transpose().dot(vector)
        residual = _reciprocal(S) * residual
        residual = S * residual
        residual = U.dot(residual)
        residual = residual - vector
        residual = np.linalg.norm(residual) / np.linalg.norm(vector)
        return residual

    def merge(self, other: "SvdMetric") -> "SvdMetric":
        # non-updating!
        return SvdMetric(self.k, self.decay, self.U, self.S)

    def to_summary_dict(self, cfg: Optional[SummaryConfig] = None) -> Dict[str, Any]:
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
    def zero(cls, cfg: Optional[MetricConfig] = None) -> "SvdMetric":
        """
        Instances created with zero() will be useless because they're
        not updatable.
        """
        cfg = cfg or SvdMetricConfig()
        if not isinstance(cfg, SvdMetricConfig):
            raise ValueError("SvdMetric.zero() requires SvdMetricConfig argument")

        return SvdMetric(
            k=IntegralComponent(0),
            decay=FractionalComponent(0.0),
            # TODO: make this mergeable?
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
        if U0.shape == (1, 1):
            U0 = np.zeros((U1.shape[0], k))
            S0 = np.zeros(k)
        if U0.shape[0] < U1.shape[0]:
            U0 = np.pad(U0, ((0, U1.shape[0] - U0.shape[0]), (0, 0)), "constant")
        assert U0.shape[0] == U1.shape[0]

        Q, R = np.linalg.qr(np.concatenate((decay * U0 * S0, U1 * S1), axis=1))
        UR, S, VRT = sp.sparse.linalg.svds(R, k, return_singular_vectors = "u")
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
            # U1, S1, _ = np.linalg.svd(vector.reshape((vector.shape[0], 1)), False, True, False)
            U1, S1 = vector.reshape((vector.shape[0], 1)), np.array([[1]])
            new_U, new_S = self._resketch(k, decay, U1, S1)
            self.U.set(new_U)
            self.S.set(new_S)

        return OperationResult.ok(vectors_processed)

    @classmethod
    def zero(cls, cfg: Optional[SvdMetricConfig] = None) -> "UpdatableSvdMetric":
        cfg = cfg or SvdMetricConfig()
        if not isinstance(cfg, SvdMetricConfig):
            raise ValueError("UpdatableSvdMetric.zero() requires SvdMetricConfig argument")

        return UpdatableSvdMetric(
            k=IntegralComponent(cfg.k),
            decay=FractionalComponent(cfg.decay),
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

    # The default will not allow updates or residual computation
    svd: SvdMetric = field(default_factory = SvdMetric.zero)


@dataclass
class NlpMetric(MultiMetric):
    """
    Natural language processing metric
    """

    svd: SvdMetric  # use an UpdatableSvdMetric to train while tracking, or SvdMetric if SVD is to be static
    fi_disabled: bool = False

    def __post_init__(self):
        submetrics = {
            "doc_length": {
                "distribuion": StandardMetric.distribution.zero(),
                "counts": StandardMetric.counts.zero(),
                "types": StandardMetric.types.zero(),
                "cardinality": StandardMetric.cardinality.zero(),
                "ints": StandardMetric.ints.zero(),
            },
            "term_length": {
                "distribution": StandardMetric.distribution.zero(),
                "counts": StandardMetric.counts.zero(),
                "types": StandardMetric.types.zero(),
                "cardinality": StandardMetric.cardinality.zero(),
                "ints": StandardMetric.ints.zero(),
            },
            "residual": {
                "distribuion": StandardMetric.distribution.zero(),
                "counts": StandardMetric.counts.zero(),
                "types": StandardMetric.types.zero(),
                "cardinality": StandardMetric.cardinality.zero(),
            },
            "frequent_terms": {
                "frequent_items": StandardMetric.frequent_items.zero(),
                "counts": StandardMetric.counts.zero(),
                "types": StandardMetric.types.zero(),
                "cardinality": StandardMetric.cardinality.zero(),
            },
        }
        if not self.fi_disabled:
            submetrics["frequent_terms"] = {
                "frequent_items": StandardMetric.frequent_items.zero(),
                "counts": StandardMetric.counts.zero(),
                "types": StandardMetric.types.zero(),
                "cardinality": StandardMetric.cardinality.zero(),
            }
            for key in ["doc_length", "term_length"]:
                submetrics[key]["frequent_items"] = StandardMetric.frequent_items.zero()

        super(NlpMetric, self).__init__(submetrics)
        super(NlpMetric, self).__post_init__()

    @property
    def namespace(self) -> str:
        return "nlp"

    def merge(self, other: "NlpMetric") -> "NlpMetric":
        result = super(NlpMetric, self).merge(other)  # update all of our submetrics
        result.svd = self.svd.merge(other.svd)  # update if self.svd is updatable, else no-op
        return result

    # MultiMetric {to,from}_protobuf(), to_summary_dict() -- you have to serialize NlpMetric.svd yourself if it updated

    def _update_submetrics(self, submetric: str, data: PreprocessedColumn) -> None:
        for key in self.submetrics[submetric].keys():
            self.submetrics[submetric][key].columnar_update(data)

    # data.list.strings is the list of strings in a (single) document
    # data.list.objs is as list of np.ndarray. Each ndarray represents one document's term vector.

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        terms = data.list.strings
        if terms:
            term_lengths = [len(term) for term in terms]
            self._update_submetrics("term_length", PreprocessedColumn.apply(term_lengths))
            self._update_submetrics("doc_length", PreprocessedColumn.apply([len(terms)]))
            if not self.fi_disabled:
                self._update_submetrics("frequent_terms", PreprocessedColumn.apply(terms))

        self.svd.columnar_update(data)  # no-op if SVD is not updating
        residuals: List[float] = []
        for vector in data.list.objs:  # TODO: batch these
            assert isinstance(vector, np.ndarray)
            residuals.append(self.svd.residual(vector))

        self._update_submetrics("residual", PreprocessedColumn.apply(residuals))
        return OperationResult.ok(1)

    @classmethod
    def zero(cls, cfg: Optional[MetricConfig] = None) -> "NlpMetric":
        cfg = cfg or NlpConfig()
        if not isinstance(cfg, NlpConfig):
            raise ValueError("NlpMetric.zero() requires an NlpConfig argument")

        return NlpMetric(cfg.svd, cfg.fi_disabled)

    @classmethod
    def from_protobuf(cls, msg: MetricMessage) -> "NlpMetric":
        submetrics = cls.submetrics_from_protobuf(msg)
        result = NlpMetric(SvdMetric.zero(SvdMetricConfig(0, 1.0)))  # not updatable, can't compute residuals
        result.submetrics = submetrics
        return result


def _preprocessifier(terms: List[str], vector: np.ndarray) -> PreprocessedColumn:
    strings = terms
    objs = [vector]
    list_view = ListView(strings=strings, objs=objs)
    result = PreprocessedColumn()
    result.list = list_view
    return result


def log_nlp(
    terms: Optional[List[str]] = None,
    vector: Optional[np.ndarray] = None,
    column_name: Optional[str] = None,
    schemas: Optional[DatasetSchema] = None,
) -> ResultSet:
    column_name = column_name or "nlp"

    class NlpResolver(Resolver):
        def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
            return {NlpMetric.get_namespace(): ImageMetric.zero(column_schema.cfg)}

    schema = schema or DatasetSchema(
        types={key: ImageType for key in images.keys()}, default_configs=ImageMetricConfig(), resolvers=ImageResolver()
    )
    if not isinstance(schema.default_configs, NlpConfig):
        raise ValueError("log_nlp requires DatasetSchema with an NlpConfig as default_configs")

    return why.log(row=images, schema=schema)
