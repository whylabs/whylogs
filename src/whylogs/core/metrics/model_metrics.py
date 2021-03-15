from typing import List, Union

from whylogs.core.metrics.confusion_matrix import ConfusionMatrix
from whylogs.core.metrics.regression_metrics import RegressionMetrics
from whylogs.proto import ModelMetricsMessage, ModelType


class ModelMetrics:
    """
    Container class for various model-related metrics

    Attributes:
        confusion_matrix (ConfusionMatrix): ConfusionMatrix which keeps it track of counts with NumberTracker
        regression_metrics (RegressionMetrics): Regression Metrics keeps track of a common regression metrics in case the targets are continous.
    """

    def __init__(self, confusion_matrix: ConfusionMatrix = None,
                 regression_metrics: RegressionMetrics = None,
                 model_type: ModelType = ModelType.UNKNOWN):
        if confusion_matrix is None:
            confusion_matrix = ConfusionMatrix()
        self.confusion_matrix = confusion_matrix
        if regression_metrics is None:
            regression_metrics = RegressionMetrics()
        self.regression_metrics = regression_metrics
        self.model_type = ModelType.UNKNOWN

    def to_protobuf(self, ) -> ModelMetricsMessage:
        return ModelMetricsMessage(
            scoreMatrix=self.confusion_matrix.to_protobuf() if self.confusion_matrix else None,
            regressionMetrics=self.regression_metrics.to_protobuf() if self.regression_metrics else None,
            modelType=self.model_type)

    @classmethod
    def from_protobuf(cls, message, ):
        return ModelMetrics(
            confusion_matrix=ConfusionMatrix.from_protobuf(message.scoreMatrix),
            regression_metrics=RegressionMetrics.from_protobuf(message.regressionMetrics),
            model_type=message.modelType)

    def compute_confusion_matrix(self, predictions: List[Union[str, int, bool, float]],
                                 targets: List[Union[str, int, bool, float]],
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

    def compute_regression_metrics(self, predictions: List[float],
                                   targets: List[float],
                                   target_field: str = None,
                                   prediction_field: str = None):
        regression_metrics = RegressionMetrics(target_field=target_field, prediction_field=prediction_field)
        regression_metrics.add(predictions, targets)
        self.regression_metrics = self.regression_metrics.merge(regression_metrics)

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

        if self.model_type is None or other.model_type is None:
            model_type = ModelType.UNKNOWN
        elif other.model_type != self.model_type:
            model_type = ModelType.UNKNOWN
        else:
            model_type = self.model_type

        return ModelMetrics(
            confusion_matrix=self.confusion_matrix.merge(other.confusion_matrix),
            regression_metrics=self.regression_metrics.merge(other.regression_metrics),
            model_type=model_type)
