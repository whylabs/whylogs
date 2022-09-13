from logging import getLogger
from typing import Dict, List, Optional, Tuple, Union

import whylogs_sketching as ds  # type: ignore

from whylogs.core.metrics.metric_components import FractionalComponent, KllComponent
from whylogs.core.metrics.metrics import DistributionMetric, MetricConfig
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto.v0 import (
    DoublesMessage,
    NumbersMessageV0,
    ScoreMatrixMessage,
    VarianceMessage,
)

MODEL_METRICS_MAX_LABELS = 256
MODEL_METRICS_LABEL_SIZE_WARNING_THRESHOLD = 64
EMPTY_KLL: bytes = ds.kll_doubles_sketch(k=128).serialize()
_empty_theta_union = ds.theta_union()
_empty_theta_union.update(ds.update_theta_sketch())
EMPTY_THETA: bytes = _empty_theta_union.get_result().serialize()

_logger = getLogger("whylogs")


def _encode_to_integers(values, uniques):
    table = {val: i for i, val in enumerate(uniques)}
    for v in values:
        if v not in uniques:
            raise ValueError("Can not encode values not in unique set")
    return [table[v] for v in values]


class ConfusionMatrix:
    """

    Confusion Matrix Class to hold labels and matrix data.

    Attributes:
        labels: list of labels in a sorted order
    """

    def __init__(
        self,
        labels: List[Union[str, int, bool, float]] = None,
    ):
        if labels:
            labels_size = len(labels)
            if labels_size > MODEL_METRICS_LABEL_SIZE_WARNING_THRESHOLD:
                _logger.warning(
                    f"The initialized confusion matrix has {labels_size} labels and the resulting"
                    " confusion matrix will be larger than is recommended with whylogs current"
                    " representation of the model metric for a confusion matrix of this size."
                )
            if labels_size > MODEL_METRICS_MAX_LABELS:
                raise ValueError(
                    f"The initialized confusion matrix has {labels_size} labels and the resulting"
                    " confusion matrix will be larger than is supported by whylogs current"
                    " representation of the model metric for a confusion matrix of this size,"
                    " selectively log the most important labels or configure the threshold of"
                    " {MODEL_METRICS_MAX_LABELS} higher by setting MODEL_METRICS_MAX_LABELS."
                )

            self.labels = sorted(labels)
        else:
            self.labels = list()

        self.confusion_matrix: Dict[Tuple[int, int], DistributionMetric] = dict()
        self.default_config = MetricConfig()

    def add(
        self,
        predictions: List[Union[str, int, bool, float]],
        targets: List[Union[str, int, bool, float]],
        scores: Optional[List[float]],
    ):
        """
        Function adds predictions and targets to confusion matrix with scores.

        Args:
            predictions (List[Union[str, int, bool]]):
            targets (List[Union[str, int, bool]]):
            scores (List[float]):

        Raises:
            NotImplementedError: in case targets do not fall into binary or
            multiclass suport
            ValueError: incase missing validation or predictions
        """
        if not isinstance(targets, list):
            targets = [targets]
        if not isinstance(predictions, list):
            predictions = [predictions]

        if scores is None:
            scores = [1.0 for _ in range(len(targets))]

        if len(targets) != len(predictions):
            raise ValueError("both targets and predictions need to have the same length")

        targets_indx = _encode_to_integers(targets, self.labels)
        prediction_indx = _encode_to_integers(predictions, self.labels)

        for ind in range(len(predictions)):
            entry_key = prediction_indx[ind], targets_indx[ind]
            if entry_key not in self.confusion_matrix:
                self.confusion_matrix[entry_key] = DistributionMetric.zero(self.default_config)
            data = PreprocessedColumn.apply([scores[ind]])
            self.confusion_matrix[entry_key].columnar_update(data)

    def merge(self, other_cm):
        """
        Merge two seperate confusion matrix which may or may not overlap in labels.

        Args:
              other_cm (Optional[ConfusionMatrix]): confusion_matrix to merge with self
        Returns:
              ConfusionMatrix: merged confusion_matrix
        """
        # TODO: always return new objects
        if other_cm is None:
            return self
        if self.labels is None or self.labels == []:
            return other_cm
        if other_cm.labels is None or other_cm.labels == []:
            return self

        # the union of the labels potentially creates a new encoding
        labels = list(set(self.labels + other_cm.labels))

        conf_matrix = ConfusionMatrix(labels)

        conf_matrix = _merge_CM(self, conf_matrix)
        conf_matrix = _merge_CM(other_cm, conf_matrix)

        return conf_matrix

    @staticmethod
    def _dist_to_numbers(dist: Optional[DistributionMetric]) -> NumbersMessageV0:

        variance_message = VarianceMessage()

        if dist is None or dist.kll.value.is_empty():
            return NumbersMessageV0(histogram=EMPTY_KLL, compact_theta=EMPTY_THETA, variance=variance_message)

        variance_message = VarianceMessage(count=dist.n, sum=dist.m2.value, mean=dist.mean.value)
        return NumbersMessageV0(
            histogram=dist.kll.value.serialize(),
            compact_theta=EMPTY_THETA,
            variance=variance_message,
            doubles=DoublesMessage(count=dist.n),
        )

    @staticmethod
    def _numbers_to_dist(numbers: NumbersMessageV0) -> DistributionMetric:
        doubles_sk = ds.kll_doubles_sketch.deserialize(numbers.histogram)
        return DistributionMetric(
            kll=KllComponent(doubles_sk),
            mean=FractionalComponent(numbers.variance.mean),
            m2=FractionalComponent(numbers.variance.sum),
        )

    def to_protobuf(
        self,
    ):
        """
        Convert to protobuf

        Returns:
            TYPE: Description
        """
        size = len(self.labels)
        if size == 0:
            return None
        confusion_matrix_entries: List[NumbersMessageV0] = []
        for i in range(size):
            for j in range(size):
                entry_key = i, j
                entry = self.confusion_matrix.get(entry_key)
                numbers_message = ConfusionMatrix._dist_to_numbers(entry)
                confusion_matrix_entries.append(numbers_message)

        return ScoreMatrixMessage(
            labels=[str(i) for i in self.labels],
            scores=confusion_matrix_entries,
        )

    @classmethod
    def from_protobuf(
        cls,
        message: ScoreMatrixMessage,
    ):
        if message is None or message.ByteSize() == 0:
            return None
        labels = message.labels
        num_labels = len(labels)
        matrix = dict()
        for i in range(num_labels):
            for j in range(num_labels):
                index = i * num_labels + j
                entry_key = i, j
                entry = message.scores[index]
                matrix[entry_key] = ConfusionMatrix._numbers_to_dist(entry)

        cm_instance = ConfusionMatrix(
            labels=labels,
        )
        cm_instance.confusion_matrix = matrix

        return cm_instance


def _merge_CM(old_conf_matrix: ConfusionMatrix, new_conf_matrix: ConfusionMatrix):
    """
    Merges two confusion_matrix since distinc or overlaping labels

    Args:
        old_conf_matrix (ConfusionMatrix)
        new_conf_matrix (ConfusionMatrix): Will be overridden
    """
    new_indxes = _encode_to_integers(old_conf_matrix.labels, new_conf_matrix.labels)
    old_indxes = _encode_to_integers(old_conf_matrix.labels, old_conf_matrix.labels)

    res_conf_matrix = ConfusionMatrix(new_conf_matrix.labels)

    res_conf_matrix.confusion_matrix = new_conf_matrix.confusion_matrix

    for old_row_idx, each_row_indx in enumerate(new_indxes):
        for old_column_idx, each_column_inx in enumerate(new_indxes):
            new_entry = new_conf_matrix.confusion_matrix.get((each_row_indx, each_column_inx))
            old_entry = old_conf_matrix.confusion_matrix.get((old_indxes[old_row_idx], old_indxes[old_column_idx]))
            res_conf_matrix.confusion_matrix[each_row_indx, each_column_inx] = (
                new_entry.merge(old_entry) if new_entry else old_entry
            )

    return res_conf_matrix
