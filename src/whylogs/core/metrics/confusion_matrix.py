import numpy as np
from typing import List, Union
from whylogs.proto import ConfusionMatrixSummary
from whylogs.core.statistics import NumberTracker


class ConfusionMatrix:

    """
    Confusion Matrix Class to hold labels and matrix data

    Attributes:
        confusion_matrix (nd.array): Description
        labels (List[str]): Description
    """

    def __init__(self, labels: List[str] = None):
        self.labels = labels
        num_labels = len(self.labels)
        self.confusion_matrix = np.empty(
            [num_labels, num_labels], dtype=object)
        for each_ind_i in range(num_labels):
            for each_ind_j in range(num_labels):
                self.confusion_matrix[each_ind_i, each_ind_j] = NumberTracker()

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
        if not isinstance(targets, list):
            targets = [targets]
        if not isinstance(predictions, list):
            predictions = [predictions]

        if not scores:
            scores = [1.0 for _ in range(len(targets))]

        if len(targets) != len(predictions):
            raise ValueError(
                "both targets and predictions need to have the same length")

        targets_indx = enconde_to_integers(targets, self.labels)
        prediction_indx = enconde_to_integers(predictions, self.labels)

        for ind in range(len(predictions)):
            # print(self.labels)
            # print(prediction_indx[ind], " ", targets_indx[ind])
            self.confusion_matrix[prediction_indx[ind],
                                  targets_indx[ind]].track(scores[ind])

    def merge(self, other_CM):

        current_labels = self.labels
        other_labels = other_CM.labels
        combined_labels = list(set(current_labels+other_labels))

        conf_Matrix = ConfusionMatrix(combined_labels)

        _merge_CM(self, conf_Matrix)
        _merge_CM(other_CM, conf_Matrix)

        return conf_Matrix

    def to_protobuf(self, ):
        return ConfusionMatrixSummary(self.labels, list(np.ravel(self.confusion_matrix)))

    def from_protobuf(self, message,):
        labels = message.labels
        num_labels = len(labels)
        matrix = np.array(message.confusion_matrix).reshape(
            (num_labels, num_labels))

        CM_instance = ConfusionMatrix(message.labels)
        CM_instance.confusion_matrix = matrix

        return CM_instance


def _merge_CM(old_conf_Matrix, new_conf_Matrix):

    new_indxes = enconde_to_integers(
        old_conf_Matrix.labels, new_conf_Matrix.labels)
    old_indxes = enconde_to_integers(
        old_conf_Matrix.labels, old_conf_Matrix.labels)

    for old_row_idx, each_row_indx in new_indxes:
        for old_column_idx, each_column_inx in new_indxes:

            new_conf_Matrix.confusion_matrix[each_row_indx,
                                             each_column_inx].merge(new_conf_Matrix.confusion_matrix[old_indxes[old_row_idx], old_indxes[old_column_idx]])


def enconde_to_integers(values, uniques):
    table = {val: i for i, val in enumerate(uniques)}
    return np.array([table[v] for v in values])
