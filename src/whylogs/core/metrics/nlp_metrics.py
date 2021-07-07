from typing import List, Union

try:
    import jiwer
except ImportError as e:
    logger.debug(str(e))
    logger.debug("Unable to load jiwer; install jiwer for partial nlp metric support")
from whylogs.core.statistics import NumberTracker
from whylogs.proto import NLPMetricsMessage


class NLPMetrics:
    def __init__(self, prediction_field: str = None, target_field: str = None):
        self.prediction_field = prediction_field
        self.target_field = target_field

    def update(self, predictions: Union[List[str], str], targets: Union[List[str]], transform=None) -> None:
        """
        Function adds predictions and targets computation of nlp metrics.

        Args:
            predictions (Union[str,List[str]]):
            targets (Union[List[str],str]):

        """
        if transform:
            mes = jiwer.compute_measures(truth=targets, hypothesis=predictions, truth_transform=transform, hypothesis_transform=transform)
        else:
            mes = jiwer.compute_measures(truth=targets, hypothesis=predictions)

        self.mer.update(mes["mer"])
        self.wer.update(mes["wer"])
        self.wil.update(mes["wil"])

    def merge(self, other: "NLPMetrics") -> "NLPMetrics":
        """
        Merge two seperate nlp metrics

        Args:
              other : nlp metrics to merge with self
        Returns:
              NLPMetrics: merged nlp metrics
        """
        if other is None:
            return self

        merged_nlp_metrics = NLPMetrics()
        merged_nlp_metrics.mer.merge(other.mer)
        merged_nlp_metrics.wer.merge(other.wer)
        merged_nlp_metrics.wil.merge(other.wil)

        return merged_nlp_metrics

    def to_protobuf(
        self,
    ):
        """
        Convert to protobuf

        Returns:
            TYPE: Protobuf Message
        """

        return NLPMetricsMessage(
            mer=self.mer,
            wer=self.wer,
            wil=self.wil,
        )

    @classmethod
    def from_protobuf(
        cls,
        message: NLPMetricsMessage,
    ):
        if message.ByteSize() == 0:
            return None

        nlp_met = NLPMetrics()
        NLPMetrics.wer = NumberTracker.from_protobuf(NLPMetricsMessage.wer)
        NLPMetrics.wil = NumberTracker.from_protobuf(NLPMetricsMessage.wil)
        NLPMetrics.mer = NumberTracker.from_protobuf(NLPMetricsMessage.mer)

        return nlp_met
