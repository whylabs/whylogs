import logging
from typing import List, Union

from whylogs.core.statistics import NumberTracker
from whylogs.proto import NLPMetricsMessage

logger = logging.getLogger(__name__)
try:
    import jiwer
except ImportError as e:
    logger.debug(str(e))
    logger.debug("Unable to load jiwer; install jiwer for partial nlp metric support")


class NLPMetrics:
    def __init__(self, prediction_field: str = None, target_field: str = None):
        self.prediction_field = prediction_field
        self.target_field = target_field
        self.mer = NumberTracker()
        self.wer = NumberTracker()
        self.wil = NumberTracker()

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

        self.mer.track(mes["mer"])
        self.wer.track(mes["wer"])
        self.wil.track(mes["wil"])

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
        merged_nlp_metrics.mer = self.mer.merge(other.mer)
        merged_nlp_metrics.wer = self.wer.merge(other.wer)
        merged_nlp_metrics.wil = self.wil.merge(other.wil)

        return merged_nlp_metrics

        return merged_nlp_metrics

    def to_protobuf(
        self,
    ) -> NLPMetricsMessage:
        """
        Convert to protobuf

        Returns:
            TYPE: Protobuf Message
        """

        return NLPMetricsMessage(
            mer=self.mer.to_protobuf(),
            wer=self.wer.to_protobuf(),
            wil=self.wil.to_protobuf(),
        )

    @classmethod
    def from_protobuf(
        cls: "NLPMetrics",
        message: NLPMetricsMessage,
    ):

        nlp_met = NLPMetrics()
        nlp_met.wer = NumberTracker.from_protobuf(message.wer)
        nlp_met.wil = NumberTracker.from_protobuf(message.wil)
        nlp_met.mer = NumberTracker.from_protobuf(message.mer)

        return nlp_met
