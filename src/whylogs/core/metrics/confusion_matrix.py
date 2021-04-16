from typing import List, Union

import numpy as np
from sklearn.utils.multiclass import type_of_target

from whylogs.core.statistics import NumberTracker
from whylogs.proto import ScoreMatrixMessage

SUPPORTED_TYPES = ("binary", "multiclass")


class ConfusionMatrix:
    """

    Confusion Matrix Class to hold labels and matrix data.

    Attributes:
        labels: list of labels in a sorted order
        prediction_field: name of the prediction field
        target_field: name of the target field
        score_field: name of the score field
        confusion_matrix (nd.array): Confusion Matrix kept as matrix of NumberTrackers
        labels (List[str]): list of labels for the confusion_matrix axes
    """

    def __init__(
        self,
        labels: List[str] = None,
        prediction_field: str = None,
        target_field: str = None,
        score_field: str = None,
    ):
        self.prediction_field = prediction_field
        self.target_field = target_field
        self.score_field = score_field
        if labels:
            self.labels = sorted(labels)
            num_labels = len(self.labels)
            self.confusion_matrix = np.empty([num_labels, num_labels], dtype=object)
            for each_ind_i in range(num_labels):
                for each_ind_j in range(num_labels):
                    self.confusion_matrix[each_ind_i, each_ind_j] = NumberTracker()
        else:
            self.labels = None
            self.confusion_matrix = None

    def add(
        self,
        predictions: List[Union[str, int, bool]],
        targets: List[Union[str, int, bool]],
        scores: List[float],
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
        tgt_type = type_of_target(targets)
        if tgt_type not in ("binary", "multiclass"):
            raise NotImplementedError("target type not supported yet")

        if not isinstance(targets, list):
            targets = [targets]
        if not isinstance(predictions, list):
            predictions = [predictions]

        if scores is None:
            scores = [1.0 for _ in range(len(targets))]

        if len(targets) != len(predictions):
            raise ValueError("both targets and predictions need to have the same length")

        targets_indx = encode_to_integers(targets, self.labels)
        prediction_indx = encode_to_integers(predictions, self.labels)

        for ind in range(len(predictions)):
            self.confusion_matrix[prediction_indx[ind], targets_indx[ind]].track(scores[ind])

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

        labels = list(set(self.labels + other_cm.labels))

        conf_matrix = ConfusionMatrix(labels)

        conf_matrix = _merge_CM(self, conf_matrix)
        conf_matrix = _merge_CM(other_cm, conf_matrix)

        return conf_matrix

    def to_protobuf(
        self,
    ):
        """
        Convert to protobuf

        Returns:
            TYPE: Description
        """
        return ScoreMatrixMessage(
            labels=self.labels,
            prediction_field=self.prediction_field,
            target_field=self.target_field,
            score_field=self.score_field,
            scores=[nt.to_protobuf() if nt else NumberTracker.to_protobuf(NumberTracker()) for nt in np.ravel(self.confusion_matrix)],
        )

    @classmethod
    def from_protobuf(
        cls,
        message: ScoreMatrixMessage,
    ):
        if message.ByteSize() == 0:
            return None
        labels = message.labels
        num_labels = len(labels)
        matrix = np.array([NumberTracker.from_protobuf(score) for score in message.scores]).reshape((num_labels, num_labels)) if num_labels > 0 else None

        cm_instance = ConfusionMatrix(
            labels=labels,
            prediction_field=message.prediction_field,
            target_field=message.target_field,
            score_field=message.score_field,
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
    new_indxes = encode_to_integers(old_conf_matrix.labels, new_conf_matrix.labels)
    old_indxes = encode_to_integers(old_conf_matrix.labels, old_conf_matrix.labels)

    res_conf_matrix = ConfusionMatrix(new_conf_matrix.labels)

    res_conf_matrix.confusion_matrix = new_conf_matrix.confusion_matrix

    for old_row_idx, each_row_indx in enumerate(new_indxes):
        for old_column_idx, each_column_inx in enumerate(new_indxes):
            res_conf_matrix.confusion_matrix[each_row_indx, each_column_inx] = new_conf_matrix.confusion_matrix[each_row_indx, each_column_inx].merge(
                old_conf_matrix.confusion_matrix[old_indxes[old_row_idx], old_indxes[old_column_idx]]
            )

    return res_conf_matrix


def encode_to_integers(values, uniques):
    table = {val: i for i, val in enumerate(uniques)}
    for v in values:
        if v not in uniques:
            raise ValueError("Can not encode values not in uniques")
    return np.array([table[v] for v in values])
