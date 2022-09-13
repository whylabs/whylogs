from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any, Dict, List, Optional

from whylogs.api.reader import Reader, Readers
from whylogs.api.writer import Writer, Writers
from whylogs.api.writer.writer import Writable
from whylogs.core import DatasetProfile, DatasetProfileView, Segment
from whylogs.core.metrics.metrics import Metric
from whylogs.core.model_performance_metrics import ModelPerformanceMetrics
from whylogs.core.segmentation_partition import SegmentationPartition
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView

logger = getLogger(__name__)


class ResultSetWriter:
    """
    Result of a logging call.
    A result set might contain one or multiple profiles or profile views.
    """

    def __init__(self, results: "ResultSet", writer: Writer):
        self._result_set = results
        self._writer = writer

    def option(self, **kwargs: Any) -> "ResultSetWriter":
        self._writer.option(**kwargs)
        return self

    def write(self, **kwargs: Any) -> None:
        # multi-profile writer
        files = self._result_set.get_writables()
        if not files:
            logger.warning("Attempt to write a result set with no writables returned, nothing written!")
        else:
            logger.debug(f"About to write {len(files)} files:")
            # TODO: special handling of large number of files, handle throttling
            for view in files:
                self._writer.write(profile=view, **kwargs)
            logger.debug(f"Completed writing {len(files)} files!")


class ResultSetReader:
    def __init__(self, reader: Reader) -> None:
        self._reader = reader

    def option(self, **kwargs: Any) -> "ResultSetReader":
        self._reader.option(**kwargs)
        return self

    def read(self, **kwargs: Any) -> "ResultSet":
        return self._reader.read(**kwargs)


class ResultSet(ABC):
    """
    A holder object for profiling results.

    A whylogs.log call can result in more than one profile. This wrapper class
    simplifies the navigation among these profiles.

    Note that currently we only hold one profile but we're planning to add other
    kinds of profiles such as segmented profiles here.
    """

    @staticmethod
    def read(multi_profile_file: str) -> "ResultSet":
        # TODO: parse multiple profile
        view = DatasetProfileView.read(multi_profile_file)
        return ViewResultSet(view=view)

    @staticmethod
    def reader(name: str = "local") -> "ResultSetReader":
        reader = Readers.get(name)
        return ResultSetReader(reader=reader)

    def writer(self, name: str = "local") -> "ResultSetWriter":
        writer = Writers.get(name)
        return ResultSetWriter(results=self, writer=writer)

    @abstractmethod
    def view(self) -> Optional[DatasetProfileView]:
        pass

    @abstractmethod
    def profile(self) -> Optional[DatasetProfile]:
        pass

    def get_writables(self) -> Optional[List[Writable]]:
        return [self.view()]

    @property
    def performance_metrics(self) -> Optional[ModelPerformanceMetrics]:
        profile = self.profile()
        if profile:
            return profile.model_performance_metrics
        return None

    def add_model_performance_metrics(self, metrics: ModelPerformanceMetrics) -> None:
        profile = self.profile()
        if profile:
            profile.add_model_performance_metrics(metrics)
        else:
            raise ValueError("Cannot add performance metrics to a result set with no profile!")

    def add_metrics(self, name: str, metric: Metric) -> None:
        profile = self.profile()
        if profile:
            profile.add_dataset_metric(name, metric)
        else:
            raise ValueError(f"Cannot add {name} metric {metric} to a result set with no profile!")


class ViewResultSet(ResultSet):
    def __init__(self, view: DatasetProfileView) -> None:
        self._view = view

    def profile(self) -> Optional[DatasetProfile]:
        raise ValueError("No profile available. Can only view")

    def view(self) -> Optional[DatasetProfileView]:
        return self._view


class ProfileResultSet(ResultSet):
    def __init__(self, profile: DatasetProfile) -> None:
        self._profile = profile

    def profile(self) -> Optional[DatasetProfile]:
        return self._profile

    def view(self) -> Optional[DatasetProfileView]:
        return self._profile.view()


class SegmentedResultSet(ResultSet):
    def __init__(
        self,
        segments: Dict[str, Dict[Segment, DatasetProfile]],
        partitions: Optional[List[SegmentationPartition]] = None,
    ) -> None:
        self._segments = segments
        self._partitions = partitions
        self.model_performance_metric: Optional[ModelPerformanceMetrics] = None

    def profile(self, segment: Optional[Segment] = None) -> Optional[DatasetProfile]:
        if not self._segments:
            return None
        elif segment:
            paritition_segments = self._segments.get(segment.parent_id)
            return paritition_segments.get(segment) if paritition_segments else None
        # special case return a single segment if there is only one, even if not specified
        elif len(self._segments) == 1:
            for partition_id in self._segments:
                segments = self._segments.get(partition_id)
                number_of_segments = len(segments) if segments else 0
                if number_of_segments == 1:
                    single_dictionary: Dict[Segment, DatasetProfile] = segments if segments else dict()
                    for key in single_dictionary:
                        return single_dictionary[key]

        raise ValueError(
            f"A profile was requested from a segmented result set without specifying which segment to return: {self._segments}"
        )

    @property
    def partitions(self) -> Optional[List[SegmentationPartition]]:
        return self._partitions

    def segments(self, restrict_to_parition_id: Optional[str] = None) -> Optional[List[Segment]]:
        result: Optional[List[Segment]] = None
        if not self._segments:
            return result
        result = list()
        if restrict_to_parition_id:
            segments = self._segments.get(restrict_to_parition_id)
            if segments:
                for segment in segments:
                    result.append(segment)
        else:
            for partition_id in self._segments:
                for segment in self._segments[partition_id]:
                    result.append(segment)
        return result

    @property
    def count(self) -> int:
        result = 0
        if self._segments:
            for segment_key in self._segments:
                profiles = self._segments[segment_key]
                result += len(profiles)
        return result

    def segments_in_partition(self, partition: SegmentationPartition) -> Optional[Dict[Segment, DatasetProfile]]:
        return self._segments.get(partition.id)

    def view(self, segment: Optional[Segment] = None) -> Optional[DatasetProfileView]:
        result = self.profile(segment)
        return result.view() if result else None

    def get_model_performance_metrics_for_segment(self, segment: Segment) -> Optional[ModelPerformanceMetrics]:
        if segment.parent_id in self._segments:
            profile = self._segments[segment.parent_id][segment]
            if not profile:
                logger.warning(
                    f"No profile found for segment {segment} when requesting model performance metrics, returning None!"
                )
                return None
            view = profile.view()
            return view.model_performance_metrics
        return None

    def get_writables(self) -> Optional[List[Writable]]:
        results: Optional[List[Writable]] = None
        if self._segments:
            results = []
            logger.info(f"Building list of: {self.count} SegmentedDatasetProfileViews in SegmentedResultSet.")
            # TODO: handle more than one partition
            if not self.partitions:
                raise ValueError(
                    f"Building list of: {self.count} SegmentedDatasetProfileViews in SegmentedResultSet but no partitions found: {self.partitions}."
                )
            if len(self.partitions) > 1:
                logger.error(
                    f"Building list of: {self.count} SegmentedDatasetProfileViews in SegmentedResultSet but found more than one partition: "
                    f"{self.partitions}. Using first partition only!!"
                )
            first_partition = self.partitions[0]
            segments = self.segments_in_partition(first_partition)
            if segments:
                for segment_key in segments:
                    profile = segments[segment_key]
                    metric = self.get_model_performance_metrics_for_segment(segment_key)
                    if metric:
                        profile.add_model_performance_metrics(metric)
                        logger.debug(
                            f"Found model performance metrics: {metric}, adding to segmented profile: {segment_key}."
                        )

                    segmented_profile = SegmentedDatasetProfileView(
                        profile_view=profile.view(), segment=segment_key, partition=first_partition
                    )
                    results.append(segmented_profile)
            else:
                logger.warning(
                    f"Found no segments in partition: {first_partition} even though we have: {self.count} segments overall"
                )
            logger.info(f"From list of: {self.count} SegmentedDatasetProfileViews using {len(results)}")
        else:
            logger.warning(
                f"Attempt to build segmented results for writing but there are no segments in this result set: {self._segments}. returning None."
            )
        return results

    def add_metrics_for_segment(self, metrics: ModelPerformanceMetrics, segment: Segment) -> None:
        if segment.parent_id in self._segments:
            profile = self._segments[segment.parent_id][segment]
            profile.add_model_performance_metrics(metrics)
