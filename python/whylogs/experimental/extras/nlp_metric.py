from copy import deepcopy
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Dict, List, Optional, Tuple, Union

from whylogs.api.logger.result_set import ProfileResultSet, ResultSet
from whylogs.core import DatasetProfile, DatasetSchema
from whylogs.core.configs import SummaryConfig
from whylogs.core.datatypes import DataType
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.metric_components import (
    FractionalComponent,
    IntegralComponent,
)
from whylogs.core.metrics.metrics import Metric, MetricConfig, OperationResult
from whylogs.core.metrics.multimetric import MultiMetric
from whylogs.core.preprocessing import ListView, PreprocessedColumn
from whylogs.core.proto import MetricMessage
from whylogs.core.resolvers import DeclarativeResolver, Resolver
from whylogs.core.schema import ColumnSchema
from whylogs.core.stubs import np, sp
from whylogs.experimental.extras.matrix_component import MatrixComponent

_SMALL = np.finfo(float).eps


def _reciprocal(s: np.ndarray) -> np.ndarray:
    """Return pseudoinverse of singular value vector"""
    # should also zap if too small relative to s[0]
    return np.array([1 / x if x > _SMALL else 0 for x in s])


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
    U: MatrixComponent  # left singular vectors
    S: MatrixComponent  # singular values

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
        return OperationResult.ok(0)

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
            U=MatrixComponent(np.zeros((1, 1))),
            S=MatrixComponent(np.zeros(1)),
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
        UR, S, VRT = sp.sparse.linalg.svds(R, k, return_singular_vectors="u")
        U = np.dot(Q, UR)
        return U, S

    def merge(self, other: "SvdMetric") -> "UpdatableSvdMetric":
        # other can be updatable or not
        new_U, new_S = self._resketch(self.k.value, self.decay.value, other.U.value, other.S.value)
        return UpdatableSvdMetric(self.k, self.decay, MatrixComponent(new_U), MatrixComponent(new_S))

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        vectors = data.list.tensors if data.list.tensors else []
        vectors = vectors + (data.pandas.tensors.tolist() if data.pandas.tensors else [])

        if not vectors:
            return OperationResult.ok(0)
        k = self.k.value
        decay = self.decay.value
        vectors_processed = 0
        for vector in vectors:
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
            U=MatrixComponent(np.zeros((1, 1))),
            S=MatrixComponent(np.zeros(1)),
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
    svd: SvdMetric = field(default_factory=SvdMetric.zero)


def _all_strings(value: List[Any]) -> bool:
    return all([isinstance(s, str) for s in value])


@dataclass
class BagOfWordsMetric(MultiMetric):
    """
    Natural language processing metric -- treat document as a bag of words
    """

    fi_disabled: bool = False

    def __post_init__(self):
        submetrics = {
            "doc_length": {
                "distribution": StandardMetric.distribution.zero(),
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
        }
        if not self.fi_disabled:
            submetrics["frequent_terms"] = {
                "frequent_items": StandardMetric.frequent_items.zero(),
                "counts": StandardMetric.counts.zero(),
                "types": StandardMetric.types.zero(),
                "cardinality": StandardMetric.cardinality.zero(),
            }
            # for key in ["doc_length", "term_length"]:
            #    submetrics[key]["frequent_items"] = StandardMetric.frequent_items.zero()

        super().__init__(submetrics)

    @property
    def namespace(self) -> str:
        return "nlp_bow"

    def _update_submetrics(self, submetric: str, data: PreprocessedColumn) -> None:
        for key in self.submetrics[submetric].keys():
            self.submetrics[submetric][key].columnar_update(data)

    def _process_document(self, document: List[str]) -> int:
        term_lengths = [len(term) for term in document]
        self._update_submetrics("term_length", PreprocessedColumn.apply(term_lengths))
        if not self.fi_disabled:
            nlp_data = PreprocessedColumn.apply(document)
            self._update_submetrics("frequent_terms", nlp_data)

        return len(document)

    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        # Should be data.list.objs  [ List[str] ] from scalar
        #           data.pandas.obj Series[List[str]] from apply
        doc_lengths = list()
        if data.list.objs and isinstance(data.list.objs[0], list) and _all_strings(data.list.objs[0]):
            doc_lengths.append(self._process_document(data.list.objs[0]))

        if data.pandas.objs is not None:
            for document in data.pandas.objs:
                if isinstance(document, list) and _all_strings(document):
                    # TODO: batch these
                    doc_lengths.append(self._process_document(document))

        self._update_submetrics("doc_length", PreprocessedColumn.apply(doc_lengths))
        return OperationResult.ok(len(doc_lengths))

    @classmethod
    def zero(cls, cfg: Optional[MetricConfig] = None) -> "BagOfWordsMetric":
        cfg = cfg or MetricConfig()
        return BagOfWordsMetric(cfg.fi_disabled)

    @classmethod
    def from_protobuf(cls, msg: MetricMessage) -> "BagOfWordsMetric":
        submetrics = cls.submetrics_from_protobuf(msg)
        result = BagOfWordsMetric()
        result.submetrics = submetrics
        return result


@dataclass
class LsiMetric(MultiMetric):
    """
    Natural language processing -- latent sematic indexing metric
    """

    svd: SvdMetric  # use an UpdatableSvdMetric to train while tracking, or SvdMetric if SVD is to be static

    def __post_init__(self):
        submetrics = {
            "residual": {
                "distribution": StandardMetric.distribution.zero(),
                "counts": StandardMetric.counts.zero(),
                "types": StandardMetric.types.zero(),
                "cardinality": StandardMetric.cardinality.zero(),
            },
        }
        super().__init__(submetrics)

    @property
    def namespace(self) -> str:
        return "nlp_lsi"

    def merge(self, other: "LsiMetric") -> "LsiMetric":
        result = super(LsiMetric, self).merge(other)  # update all of our submetrics
        result.svd = self.svd.merge(other.svd)  # update if self.svd is updatable, else no-op
        return result

    # MultiMetric {to,from}_protobuf(), to_summary_dict() -- you have to serialize LsiMetric.svd yourself if it updated

    def _update_submetrics(self, submetric: str, data: PreprocessedColumn) -> None:
        for key in self.submetrics[submetric].keys():
            self.submetrics[submetric][key].columnar_update(data)

    # data.list.objs is a list of np.ndarray. Each ndarray represents one document's term vector.
    def columnar_update(self, data: PreprocessedColumn) -> OperationResult:
        self.svd.columnar_update(data)  # no-op if SVD is not updating
        residuals: List[float] = []
        pandas_tensors = data.pandas.tensors if data.pandas.tensors is not None else []
        for vector in chain(data.list.tensors or [], pandas_tensors):  # TODO: batch these?
            residuals.append(self.svd.residual(vector))

        self._update_submetrics("residual", PreprocessedColumn.apply(residuals))
        return OperationResult.ok(len(residuals))

    @classmethod
    def zero(cls, cfg: Optional[MetricConfig] = None) -> "LsiMetric":
        cfg = cfg or NlpConfig()
        if not isinstance(cfg, NlpConfig):
            raise ValueError("LsiMetric.zero() requires an NlpConfig argument")

        return LsiMetric(cfg.svd)

    @classmethod
    def from_protobuf(cls, msg: MetricMessage) -> "LsiMetric":
        submetrics = cls.submetrics_from_protobuf(msg)
        result = LsiMetric(SvdMetric.zero(SvdMetricConfig(0, 1.0)))  # not updatable, can't compute residuals
        result.submetrics = submetrics
        return result


class ResolverWrapper(Resolver):
    def __init__(self, resolver: Resolver):
        self._resolver = resolver

    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        # TODO: make both metrics optional?
        if name.endswith("_bag_of_words"):
            return {BagOfWordsMetric.get_namespace(): BagOfWordsMetric.zero(column_schema.cfg)}
        elif name.endswith("_lsi"):
            return {LsiMetric.get_namespace(): LsiMetric.zero(column_schema.cfg)}
        return self._resolver.resolve(name, why_type, column_schema)


class NlpLogger:
    def __init__(
        self,
        svd_class: Optional[type] = None,  # TODO: maybe make this updatable: bool = False
        svd_config: Optional[SvdMetricConfig] = None,
        svd_state: Optional[MetricMessage] = None,
        schema: Optional[DatasetSchema] = None,
        column_prefix: str = "nlp",
    ):
        if svd_class:
            svd_config = svd_config or SvdMetricConfig()
            if svd_state:
                self._svd_metric = svd_class.from_protobuf(svd_state)  # type: ignore
            else:
                self._svd_metric = svd_class.zero(svd_config)  # type: ignore
        else:
            self._svd_metric = None

        self._column_prefix = column_prefix
        datatypes: Dict[str, Any] = {f"{column_prefix}_bag_of_words": List[str]}
        if self._svd_metric:
            datatypes[f"{column_prefix}_lsi"] = np.ndarray

        if schema:
            schema = deepcopy(schema)
            schema.types.update(datatypes)
            orig_config = schema.default_configs
            schema.default_configs = NlpConfig(
                hll_lg_k=orig_config.hll_lg_k,
                kll_k=orig_config.kll_k,
                fi_lg_max_k=orig_config.fi_lg_max_k,
                fi_disabled=orig_config.fi_disabled,
                track_unicode_ranges=orig_config.track_unicode_ranges,
                large_kll_k=orig_config.large_kll_k,
                unicode_ranges=orig_config.unicode_ranges,
                lower_case=orig_config.lower_case,
                normalize=orig_config.normalize,
                svd=self._svd_metric,
            )
        else:
            schema = DatasetSchema(
                types=datatypes,
                default_configs=NlpConfig(svd=self._svd_metric),
                resolvers=ResolverWrapper(DeclarativeResolver()),
            )

        self._profile = DatasetProfile(schema=schema)

    def log(
        self,
        # TODO: will add obj, pandas, row here eventually
        terms: Optional[Union[Dict[str, List[str]], List[str]]] = None,  # bag of words
        vector: Optional[Union[Dict[str, np.ndarray], np.ndarray]] = None,  # term vector representing document
    ) -> ResultSet:
        if terms:
            column_data = PreprocessedColumn.apply(terms)
            bow_metric = self._profile._columns[f"{self._column_prefix}_bag_of_words"]._metrics[
                BagOfWordsMetric.get_namespace()
            ]
            bow_metric.columnar_update(column_data)

        if vector is not None and self._svd_metric:
            # TODO: if vector and not self._svd_metric: logger.warning("no vector space metric configured")
            objs = [vector]
            list_view = ListView(objs=objs)
            column_data = PreprocessedColumn()
            column_data.list = list_view
            lsi_metric = self._profile._columns[f"{self._column_prefix}_lsi"]._metrics[LsiMetric.get_namespace()]
            lsi_metric.columnar_update(column_data)

        return ProfileResultSet(self._profile)

    def get_svd_state(self) -> MetricMessage:
        return self._svd_metric.to_protobuf()

    def get_profile(self) -> ResultSet:
        return ProfileResultSet(self._profile)
