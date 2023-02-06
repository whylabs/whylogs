from abc import ABC, abstractmethod
from whylogs.api.logger.result_set import SegmentedResultSet
from whylogs.core.stubs import pd
from typing import Union, Tuple
from logging import getLogger
from whylogs.core import Segment
from whylogs.core.model_performance_metrics.confusion_matrix import ConfusionMatrix

logger = getLogger(__name__)


class PerformanceEstimator(ABC):
    def __init__(self, reference_result_set: SegmentedResultSet):
        self._reference_result_set = reference_result_set


class AccuracyEstimator(PerformanceEstimator):
    def __init__(self, reference_result_set: SegmentedResultSet):
        super().__init__(reference_result_set)

    def _get_segments(self, reference_results: SegmentedResultSet, target_results: SegmentedResultSet):
        if len(reference_results.partitions) > 1 and len(target_results.partitions) > 1:
            logger.warning("More than one partition found. Only the first partition will be used for the estimation.")
        if len(reference_results.partitions) != len(target_results.partitions):
            raise ValueError("The number of partitions in the reference and target results must be the same.")

        reference_partition = reference_results.partitions[0]
        target_partition = target_results.partitions[0]

        segmented_column = reference_partition.name
        if segmented_column != target_partition.name:
            raise ValueError("The segmented columns in the reference and target results must be the same.")

        reference_segments = reference_results.segments_in_partition(reference_partition)
        target_segments = target_results.segments_in_partition(target_partition)

        if any([len(segment.key) > 1 for segment in reference_segments]):
            raise ValueError("Only single key segments are supported.")
        if any([len(segment.key) > 1 for segment in target_segments]):
            raise ValueError("Only single key segments are supported.")

        reference_keys = set([segment.key[0] for segment in reference_segments])
        target_keys = set([segment.key[0] for segment in target_segments])
        if reference_keys != target_keys:
            raise ValueError("The keys in the reference and target segments must be the same.")

        return reference_segments, target_segments

    def _get_cell_from_confusion_matrix(self, reference_confusion_matrix: ConfusionMatrix, key: Tuple[int, int]):
        confusion_matrix = reference_confusion_matrix.confusion_matrix
        dist_cell = confusion_matrix.get(key, None)
        return dist_cell.n if dist_cell is not None else 0

    def _get_reference_accuracies_per_segment(self, reference_segment: Segment):
        id = reference_segment.parent_id
        reference_conf: ConfusionMatrix = self._reference_result_set._segments[id][
            reference_segment
        ].model_performance_metrics.confusion_matrix
        tp = self._get_cell_from_confusion_matrix(reference_conf, (1, 1))
        tn = self._get_cell_from_confusion_matrix(reference_conf, (0, 0))
        fp = self._get_cell_from_confusion_matrix(reference_conf, (0, 1))
        fn = self._get_cell_from_confusion_matrix(reference_conf, (1, 0))
        reference_acc = (tp + tn) / (tp + tn + fp + fn)

        return reference_acc

    def _get_segment_counts(self, results: SegmentedResultSet):
        partition = results.partitions[0]
        segments = results.segments_in_partition(partition)
        segmented_column = partition.name

        counts = {}
        for segment in segments:
            id = segment.parent_id
            profile = results._segments[id][segment]
            segment_count = profile._columns[segmented_column]._metrics["counts"].n.value
            counts[segment.key[0]] = segment_count
        return counts

    def estimate(self, target: Union[pd.DataFrame, SegmentedResultSet]) -> float:
        if isinstance(target, pd.DataFrame):
            raise NotImplementedError("AccuracyEstimator does not yet support pandas.DataFrame as target")
        assert isinstance(target, SegmentedResultSet), "target must be a SegmentedResultSet"

        reference_results = self._reference_result_set
        target_results = target
        reference_segments, target_segments = self._get_segments(reference_results, target_results)

        reference_accuracies = {}
        for reference_segment in reference_segments:
            reference_accuracy = self._get_reference_accuracies_per_segment(reference_segment)
            reference_accuracies[reference_segment.key[0]] = reference_accuracy

        target_counts = self._get_segment_counts(target_results)

        pass
