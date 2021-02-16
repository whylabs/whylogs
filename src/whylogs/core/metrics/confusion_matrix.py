from typing import List, Union

import numpy as np
from sklearn.utils.multiclass import type_of_target

from whylogs.proto import ScoreMatrixMessage
from whylogs.core.statistics import NumberTracker


SUPPORTED_TYPES = ("binary", "multiclass")


class ConfusionMatrix:

    """
    Confusion Matrix Class to hold labels and matrix data

    Attributes:
        confusion_matrix (nd.array): Description
        labels (List[str]): Description
    """

    def __init__(self, labels: List[str] = None,
                 prediction_field=None,
                 target_field=None,
                 score_field=None):
        self.prediction_field = prediction_field
        self.target_field = target_field
        self.score_field = score_field
        if labels:
            self.labels = sorted(labels)
            num_labels = len(self.labels)
            self.confusion_matrix = np.empty(
                [num_labels, num_labels], dtype=object)
            for each_ind_i in range(num_labels):
                for each_ind_j in range(num_labels):
                    self.confusion_matrix[each_ind_i,
                                          each_ind_j] = NumberTracker()
        else:
            self.labels = None
            self.confusion_matrix = None

    def add(self, predictions: List[Union[str, int, bool]],
            targets: List[Union[str, int, bool]],
            scores: List[float]):
        """
        add. predictions and targets to confusion matrix with scores

        Args:
            predictions (TYPE): Description
            targets (TYPE): Description
            scores (TYPE): Description

        Raises:
            ValueError: Description
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
            raise ValueError(
                "both targets and predictions need to have the same length")

        targets_indx = enconde_to_integers(targets, self.labels)
        prediction_indx = enconde_to_integers(predictions, self.labels)

        for ind in range(len(predictions)):
            self.confusion_matrix[prediction_indx[ind],
                                  targets_indx[ind]].track(scores[ind])

    def merge(self, other_CM):
        """
        merge two seperate confusion matrix which may or may not overlap in labels

        Args:
            other_CM (TYPE): Description

        Returns:
            TYPE: Description
        """

        if self.labels is None or self.labels == []:
            return other_CM
        if other_CM.labels is None or other_CM.labels == []:
            return self

        labels = list(set(self.labels+other_CM.labels))

        conf_Matrix = ConfusionMatrix(labels)

        _merge_CM(self, conf_Matrix)
        _merge_CM(other_CM, conf_Matrix)

        return conf_Matrix

    def to_protobuf(self, ):
        return ScoreMatrixMessage(labels=self.labels, prediction_field=self.prediction_field,
                                  target_field=self.target_field,
                                  score_field=self.score_field,
                                  scores=[nt.to_protobuf() if nt else NumberTracker.to_protobuf(NumberTracker()) for nt in np.ravel(self.confusion_matrix)])

    @classmethod
    def from_protobuf(self, message,):
        labels = message.labels
        num_labels = len(labels)
        matrix = np.array([NumberTracker.from_protobuf(score)for score in message.scores]).reshape(
            (num_labels, num_labels)) if num_labels > 0 else None

        CM_instance = ConfusionMatrix(labels=labels,
                                      prediction_field=message.prediction_field,
                                      target_field=message.target_field,
                                      score_field=message.score_field)
        CM_instance.confusion_matrix = matrix

        return CM_instance


def _merge_CM(old_conf_Matrix, new_conf_Matrix):

    new_indxes = enconde_to_integers(
        old_conf_Matrix.labels, new_conf_Matrix.labels)
    old_indxes = enconde_to_integers(
        old_conf_Matrix.labels, old_conf_Matrix.labels)

    for old_row_idx, each_row_indx in enumerate(new_indxes):
        for old_column_idx, each_column_inx in enumerate(new_indxes):

            new_conf_Matrix.confusion_matrix[each_row_indx, each_column_inx] = \
                new_conf_Matrix.confusion_matrix[each_row_indx,
                                                 each_column_inx].merge(
                old_conf_Matrix.confusion_matrix[old_indxes[old_row_idx],
                                                 old_indxes[old_column_idx]])


def enconde_to_integers(values, uniques):
    table = {val: i for i, val in enumerate(uniques)}
    return np.array([table[v] for v in values])
