from typing import List, Union

from whylogs.core.metrics.confusion_matrix import ConfusionMatrix
from whylogs.proto import ModelMetricsMessage


class ModelMetrics:
    """
    Container class for various model-related metrics

    Attributes:
        confusion_matrix (ConfusionMatrix): ConfusionMatrix which keeps it track of counts with NumberTracker
    """

    def __init__(self, confusion_matrix: ConfusionMatrix = None):
        if confusion_matrix is None:
            confusion_matrix = ConfusionMatrix()
        self.confusion_matrix = confusion_matrix

    def to_protobuf(self, ) -> ModelMetricsMessage:
        return ModelMetricsMessage(scoreMatrix=self.confusion_matrix.to_protobuf() if self.confusion_matrix else None)

    @classmethod
    def from_protobuf(cls, message, ):
        return ModelMetrics(confusion_matrix=ConfusionMatrix.from_protobuf(message.scoreMatrix))

    def compute_confusion_matrix(self, predictions: List[Union[str, int, bool]],
                                 targets: List[Union[str, int, bool]],
                                 scores: List[float] = None,
                                 target_field: str = None,
                                 prediction_field: str = None,
                                 score_field: str = None):
        """
        computes the confusion_matrix, if one is already present merges to old one.

        Args:
            predictions (List[Union[str, int, bool]]):
            targets (List[Union[str, int, bool]]):
            scores (List[float], optional):
            target_field (str, optional):
            prediction_field (str, optional):
            score_field (str, optional):
        """
        labels = sorted(list(set(targets + predictions)))
        confusion_matrix = ConfusionMatrix(labels,
                                           target_field=target_field,
                                           prediction_field=prediction_field,
                                           score_field=score_field)
        confusion_matrix.add(predictions, targets, scores)

        if self.confusion_matrix.labels is None or self.confusion_matrix.labels == []:
            self.confusion_matrix = confusion_matrix
        else:
            self.confusion_matrix = self.confusion_matrix.merge(
                confusion_matrix)

    def merge(self, other):
        """

        :type other: ModelMetrics
        """
        if other is None:
            return self
        if other.confusion_matrix is None:
            # TODO: return a copy instead
            return self
        if self.confusion_matrix is None:
            return other
        return ModelMetrics(confusion_matrix=self.confusion_matrix.merge(other.confusion_matrix))
