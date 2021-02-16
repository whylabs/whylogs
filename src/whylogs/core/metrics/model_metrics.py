from whylogs.core.metrics.confusion_matrix import ConfusionMatrix
from whylogs.proto import ModelMetricsMessage


class ModelMetrics:

    def __init__(self, confusion_matrix: ConfusionMatrix = ConfusionMatrix()):
        self.confusion_matrix = confusion_matrix

    def to_protobuf(self,):
        return ModelMetricsMessage(scoreMatrix=self.confusion_matrix.to_protobuf() if self.confusion_matrix else None)

    @classmethod
    def from_protobuf(self, message,):
        return ModelMetrics(confusion_matrix=ConfusionMatrix.from_protobuf(message.scoreMatrix))

    def compute_confusion_matrix(self, predictions, targets, scores=None, target_field=None,
                                 prediction_field=None,
                                 score_field=None):

        labels = sorted(list(set(targets+predictions)))
        confusion_matrix = ConfusionMatrix(labels,
                                           target_field=target_field,
                                           prediction_field=prediction_field,
                                           score_field=score_field)
        confusion_matrix.add(predictions, targets,  scores)

        if self.confusion_matrix.labels is None or self.confusion_matrix.labels == []:
            self.confusion_matrix = confusion_matrix
        else:
            self.confusion_matrix = self.confusion_matrix.merge(
                confusion_matrix)

    def merge(self, model_metrics):

        return ModelMetrics(confusion_matrix=self.confusion_matrix.merge(model_metrics.confusion_matrix))
