from whylogs.core.metrics.confusion_matrix import ConfusionMatrix
from whylogs.proto import ModelMetricsMessage


class Metrics:

    def __init__(self, confusion_matrix: ConfusionMatrix = None):
        self.confusion_matrix = confusion_matrix

    def to_protobuf(self,):

        return MetricsMessage(scoreMatrix=self.confusion_matrix.to_protobuf())

    @classmethod
    def from_protobuf(self, message):
        return Metrics(confusion_matrix=ConfusionMatrix().from_protobuf(message.scoreMatrix))
