from abc import ABC, abstractmethod
from logging import getLogger
from typing import Tuple

from whylogs.api.logger.result_set import SegmentedResultSet
from whylogs.core import DatasetProfile, Segment
from whylogs.core.model_performance_metrics.confusion_matrix import ConfusionMatrix
from whylogs.experimental.performance_estimation.estimation_results import (
    EstimationResult,
)

logger = getLogger(__name__)


class PerformanceEstimator(ABC):
    """
    Base class for performance estimators.
    """

    def __init__(self, reference_result_set: SegmentedResultSet):
        self._reference_result_set = reference_result_set

    @abstractmethod
    def estimate(self, target_result_set: SegmentedResultSet) -> EstimationResult:
        pass


class AccuracyEstimator(PerformanceEstimator):
    """
    Estimates the accuracy of a model based on the confusion matrix of the reference result set and the target result set.

    The estimation is based on the individual performance of each segment of the reference result set. The reference accuracies
    for each segment are weighted by the number of records in each segment in the target result set.
    The estimated accuracy is the sum of the weighted accuracies of each segment.

    Expects a single partition with mutually exclusive segments, and same set of segments in the reference and target result sets.

    Supported for binary classification only.


    Examples
    --------

    Create target and reference dataframes:

    .. code-block:: python

        from whylogs.experimental.performance_estimation import AccuracyEstimator
        from whylogs.api.logger.result_set import SegmentedResultSet

        estimator = AccuracyEstimator(reference_result_set: SegmentedResultSet = reference_results)

        estimator_result = estimator.estimate(target_result_set: SegmentedResultSet = target_results)
        print("Estimated accuracy: ", estimator_result.accuracy)
    """

    def __init__(self, reference_result_set: SegmentedResultSet):
        if len(reference_result_set.partitions) > 1:
            logger.warning("More than one partition found. Only the first partition will be used for the estimation.")
        self._reference_result_timestamp = self._get_first_segment_timestamp(reference_result_set)
        self._partition_id = reference_result_set.partitions[0].id
        self._target_result_timestamp = None
        super().__init__(reference_result_set)

    def _get_first_segment_timestamp(self, result_set: SegmentedResultSet):
        partition_id = result_set.partitions[0].id
        segment_key = next(iter(result_set._segments[partition_id]))
        return result_set._segments[partition_id][segment_key].dataset_timestamp

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

        first_result_segment = next(iter(reference_segments))
        first_result_profile = self._get_profile_from_segment(first_result_segment, reference_results)

        self._validate_profile(first_result_profile)

        if any([len(segment.key) > 1 for segment in reference_segments]):
            raise ValueError("Only single key segments are supported.")
        if any([len(segment.key) > 1 for segment in target_segments]):
            raise ValueError("Only single key segments are supported.")

        reference_keys = set([segment.key[0] for segment in reference_segments])
        target_keys = set([segment.key[0] for segment in target_segments])
        if not target_keys.issubset(reference_keys):
            raise ValueError(
                "The set of keys in the target results must be a subset of the keys in the reference results."
            )

        return reference_segments, target_segments

    def _get_cell_from_confusion_matrix(self, reference_confusion_matrix: ConfusionMatrix, key: Tuple[int, int]):
        confusion_matrix = reference_confusion_matrix.confusion_matrix
        dist_cell = confusion_matrix.get(key, None)
        return dist_cell.n if dist_cell is not None else 0

    def _get_profile_from_segment(self, segment: Segment, results: SegmentedResultSet):
        id = segment.parent_id
        return results._segments[id][segment]

    def _validate_profile(self, segment_profile: DatasetProfile):
        if segment_profile.model_performance_metrics is None:
            raise ValueError("The results do not contain model performance metrics.")
        if segment_profile.model_performance_metrics.confusion_matrix is None:
            raise ValueError("Accuracy estimation currently supported for binary classification problems.")

    def _get_reference_accuracies_per_segment(self, reference_segment: Segment):
        segment_profile = self._get_profile_from_segment(reference_segment, self._reference_result_set)
        reference_conf = segment_profile.model_performance_metrics.confusion_matrix

        tp = self._get_cell_from_confusion_matrix(reference_conf, (1, 1))
        tn = self._get_cell_from_confusion_matrix(reference_conf, (0, 0))
        fp = self._get_cell_from_confusion_matrix(reference_conf, (0, 1))
        fn = self._get_cell_from_confusion_matrix(reference_conf, (1, 0))
        reference_acc = (tp + tn) / (tp + tn + fp + fn)

        return reference_acc

    def _get_reference_timestamp(self):
        return self._reference_result_timestamp

    def _get_target_timestamp(self):
        return self._target_result_timestamp

    def _get_reference_partition_id(self):
        return self._partition_id

    def _get_segment_proportions(self, results: SegmentedResultSet):
        partition = results.partitions[0]
        segments = results.segments_in_partition(partition)
        segmented_column = partition.name

        counts = {}
        for segment in segments:
            id = segment.parent_id
            profile = results._segments[id][segment]
            segment_count = profile._columns[segmented_column]._metrics["counts"].n.value
            counts[segment.key[0]] = segment_count
        total = sum(counts.values())
        proportions = {k: v / total for k, v in counts.items()}

        return proportions

    def _estimate_accuracy_based_on_proportions(self, reference_accuracies, target_proportions):
        estimated_accuracy = sum([reference_accuracies[k] * target_proportions[k] for k in reference_accuracies.keys()])
        return estimated_accuracy

    def estimate(self, target: SegmentedResultSet) -> EstimationResult:
        """
        Estimate the overall accuracy of a target result set based on a reference result set.

        Assumes the presence of a segmented column in the reference and target result sets.

        returns: An EstimationResult object containing the estimated accuracy, partition's id and the profiles' dataset timestamp.
        """

        assert isinstance(target, SegmentedResultSet), "target must be a SegmentedResultSet"
        self._target_result_timestamp = self._get_first_segment_timestamp(target)
        reference_results = self._reference_result_set
        target_results = target
        reference_segments, _ = self._get_segments(reference_results, target_results)

        reference_accuracies = {}
        for reference_segment in reference_segments:
            reference_accuracy = self._get_reference_accuracies_per_segment(reference_segment)
            reference_accuracies[reference_segment.key[0]] = reference_accuracy

        target_proportions = self._get_segment_proportions(target_results)
        estimated_accuracy = self._estimate_accuracy_based_on_proportions(reference_accuracies, target_proportions)
        estimation_result = EstimationResult(
            accuracy=estimated_accuracy,
            reference_result_timestamp=self._get_reference_timestamp(),
            reference_partition_id=self._get_reference_partition_id(),
            target_result_timestamp=self._get_target_timestamp(),
        )
        return estimation_result
