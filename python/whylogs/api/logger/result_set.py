from abc import ABC, abstractmethod
from datetime import datetime
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple, Union

from whylabs_client.model.segment_tag import SegmentTag

from whylogs.api.reader import Reader, Readers
from whylogs.api.writer.writer import WriterWrapper, _Writable
from whylogs.core import DatasetProfile, DatasetProfileView, Segment
from whylogs.core.metrics.metrics import Metric
from whylogs.core.model_performance_metrics import ModelPerformanceMetrics
from whylogs.core.segmentation_partition import SegmentationPartition
from whylogs.core.utils import ensure_timezone
from whylogs.core.view.dataset_profile_view import _MODEL_PERFORMANCE
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView
from whylogs.migration.converters import _generate_segment_tags_metadata

logger = getLogger(__name__)


def _merge_metrics(
    lhs_metrics: Optional[Dict[str, Any]], rhs_metrics: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    if not rhs_metrics:
        return lhs_metrics
    if not lhs_metrics:
        return rhs_metrics
    lhs_keys = lhs_metrics.keys()
    rhs_keys = rhs_metrics.keys()
    merged_metrics: Dict[str, Any] = dict()
    lhs_only = lhs_keys - rhs_keys
    rhs_only = rhs_keys - lhs_keys
    intersection_keys = lhs_keys & rhs_keys
    for key in lhs_only:
        merged_metrics[key] = lhs_metrics[key]

    for key in rhs_only:
        merged_metrics[key] = rhs_metrics[key]

    for key in intersection_keys:
        merged_metrics[key] = lhs_metrics[key].merge(rhs_metrics[key])

    return merged_metrics


def _accumulate_properties(acc: Optional[Dict[str, Any]], props: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not props:
        return acc
    if acc is None:
        acc = dict()
    intersection_keys = acc.keys() & props.keys()
    for key in intersection_keys:
        if acc[key] != props[key]:
            logger.warning(f"Merging result properties collision, using {key}:{acc[key]} and dropping {props[key]}.")
    acc.update(props)
    return acc


def _merge_segments(
    lhs_segments: Dict[Segment, Union[DatasetProfile, DatasetProfileView]],
    rhs_segments: Dict[Segment, Union[DatasetProfile, DatasetProfileView]],
) -> Dict[Segment, DatasetProfileView]:
    lhs_keys = lhs_segments.keys()
    rhs_keys = rhs_segments.keys()
    merged_segments: Dict[Segment, DatasetProfileView] = dict()
    lhs_only = lhs_keys - rhs_keys
    rhs_only = rhs_keys - lhs_keys
    intersection_keys = lhs_keys & rhs_keys
    for key in lhs_only:
        left_value = lhs_segments[key]
        left_view = left_value if isinstance(left_value, DatasetProfileView) else left_value.view()
        merged_segments[key] = left_view

    for key in rhs_only:
        right_value = rhs_segments[key]
        right_view = right_value if isinstance(right_value, DatasetProfileView) else right_value.view()
        merged_segments[key] = right_view

    for key in intersection_keys:
        left_value = lhs_segments[key]
        left_view = left_value if isinstance(left_value, DatasetProfileView) else left_value.view()
        right_value = lhs_segments[key]
        right_view = right_value if isinstance(right_value, DatasetProfileView) else right_value.view()
        merged_segments[key] = left_view.merge(right_view)

    return merged_segments


def _merge_partitioned_segments(
    lhs_segments: Dict[str, Dict[Segment, DatasetProfile]], rhs_segments: Dict[str, Dict[Segment, DatasetProfile]]
) -> Dict[str, Dict[Segment, DatasetProfile]]:
    lhs_partitions = lhs_segments.keys()
    rhs_partitions = rhs_segments.keys()
    merged_partitions: Dict[str, Dict[Segment, DatasetProfile]] = dict()
    lhs_only = lhs_partitions - rhs_partitions
    rhs_only = rhs_partitions - lhs_partitions
    intersection_keys = lhs_partitions & rhs_partitions
    for key in lhs_only:
        merged_partitions[key] = lhs_segments[key]

    for key in rhs_only:
        merged_partitions[key] = rhs_segments[key]

    for key in intersection_keys:
        merged_partitions[key] = _merge_segments(lhs_segments[key], rhs_segments[key])

    return merged_partitions


def _merge_partitions(
    lhs_partitions: List[SegmentationPartition], rhs_partitions: List[SegmentationPartition]
) -> List[SegmentationPartition]:
    return list(set(lhs_partitions).union(set(rhs_partitions)))


class ResultSetReader:
    def __init__(self, reader: Reader) -> None:
        self._reader = reader

    def option(self, **kwargs: Any) -> "ResultSetReader":
        self._reader.option(**kwargs)
        return self

    def read(self, **kwargs: Any) -> "ResultSet":
        return self._reader.read(**kwargs)


def _flatten_tags(tags: Union[List, Dict]) -> List[SegmentTag]:
    if type(tags[0]) == list:
        result: List[SegmentTag] = []
        for t in tags:
            result.append(_flatten_tags(t))
        return result

    return [SegmentTag(t["key"], t["value"]) for t in tags]


class ResultSet(_Writable, ABC):
    """
    A holder object for profiling results.

    A whylogs.log call can result in more than one profile. This wrapper class
    simplifies the navigation among these profiles.

    Note that currently we only hold one profile but we're planning to add other
    kinds of profiles such as segmented profiles here.
    """

    def _get_default_filename(self) -> str:
        view = self.view()
        if view is None:
            raise ValueError("No ResultSet view available")
        return view._get_default_filename()

    def get_default_path(self) -> Optional[str]:
        view = self.view()
        if view is None:
            raise ValueError("No ResultSet view available")
        return view.get_default_path()

    def _write(
        self, path: Optional[str] = None, filename: Optional[str] = None, **kwargs: Any
    ) -> Tuple[bool, Union[str, List[str]]]:
        view = self.view()
        if view is None:
            raise ValueError("No ResultSet view available")
        return view._write(path, filename, **kwargs)

    @staticmethod
    def read(multi_profile_file: str) -> "ResultSet":
        # TODO: parse multiple profile
        view = DatasetProfileView.read(multi_profile_file)
        return ViewResultSet(view=view)

    @staticmethod
    def reader(name: str = "local") -> "ResultSetReader":
        reader = Readers.get(name)
        return ResultSetReader(reader=reader)

    @abstractmethod
    def view(self) -> Optional[DatasetProfileView]:
        pass

    @abstractmethod
    def profile(self) -> Optional[DatasetProfile]:
        pass

    @property
    def metadata(self) -> Optional[Dict[str, str]]:
        if hasattr(self, "_metadata"):
            return self._metadata
        return None

    def get_writables(self) -> Optional[List[_Writable]]:
        return [self.view()]

    def set_dataset_timestamp(self, dataset_timestamp: datetime) -> None:
        ensure_timezone(dataset_timestamp)
        profile = self.profile()
        if profile is None:
            raise ValueError("Cannot set timestamp on a result set without a profile!")
        else:
            profile.set_dataset_timestamp(dataset_timestamp)

    @property
    def count(self) -> int:
        result = 0
        if self.view() is not None:
            result = 1
        return result

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

    def add_metric(self, name: str, metric: Metric) -> None:
        profile = self.profile()
        if profile:
            profile.add_dataset_metric(name, metric)
        else:
            raise ValueError(f"Cannot add {name} metric {metric} to a result set with no profile!")

    def merge(self, other: "ResultSet") -> "ResultSet":
        raise NotImplementedError("This result set did not define merge, see ProfileResultSet or SegmentedResulSet.")


class ViewResultSet(ResultSet):
    def __init__(self, view: DatasetProfileView) -> None:
        self._view = view

    def profile(self) -> Optional[DatasetProfile]:
        raise ValueError("No profile available. Can only view")

    def view(self) -> Optional[DatasetProfileView]:
        return self._view

    @property
    def metadata(self) -> Optional[Dict[str, str]]:
        view = self.view()
        if view:
            return view.metadata
        else:
            return None

    @staticmethod
    def zero() -> "ViewResultSet":
        return ViewResultSet(DatasetProfileView.zero())

    def merge(self, other: "ResultSet") -> "ViewResultSet":
        if other is None:
            return self

        lhs_view = self._view or DatasetProfileView.zero()
        if not isinstance(other, (ViewResultSet, ProfileResultSet)):
            logger.warning(f"Merging potentially incompatible ViewResultSet and {type(other)}")
        return ViewResultSet(lhs_view.merge(other.view()))

    def set_dataset_timestamp(self, dataset_timestamp: datetime) -> None:
        ensure_timezone(dataset_timestamp)
        view = self.view()
        if view is None:
            raise ValueError("Cannot set timestamp on a view result set without a view!")
        else:
            view.dataset_timestamp = dataset_timestamp


class ProfileResultSet(ResultSet):
    def __init__(self, profile: DatasetProfile) -> None:
        self._profile = profile

    def profile(self) -> Optional[DatasetProfile]:
        return self._profile

    def view(self) -> Optional[DatasetProfileView]:
        return self._profile.view()

    @property
    def metadata(self) -> Optional[Dict[str, str]]:
        view = self.view()
        if view:
            return view.metadata
        else:
            return None

    @staticmethod
    def zero() -> "ProfileResultSet":
        return ProfileResultSet(DatasetProfile())

    def merge(self, other: "ResultSet") -> ViewResultSet:
        if other is None:
            return self

        lhs_profile = self.view() or DatasetProfileView()
        if not isinstance(other, (ProfileResultSet, ViewResultSet)):
            logger.error(f"Merging potentially incompatible ProfileResultSet and {type(other)}")
        return ViewResultSet(lhs_profile.merge(other.view()))


class SegmentedResultSet(ResultSet):
    def __init__(
        self,
        segments: Dict[str, Dict[Segment, Union[DatasetProfile, DatasetProfileView]]],
        partitions: Optional[List[SegmentationPartition]] = None,
        metrics: Optional[Dict[str, Any]] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._segments = segments
        self._partitions = partitions
        self._metrics = metrics or dict()
        self._dataset_properties = properties or dict()
        self._metadata: Dict[str, str] = dict()

    def profile(self, segment: Optional[Segment] = None) -> Optional[Union[DatasetProfile, DatasetProfileView]]:
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
                    single_dictionary: Dict[Segment, Union[DatasetProfile, DatasetProfileView]] = (
                        segments if segments else dict()
                    )
                    for key in single_dictionary:
                        return single_dictionary[key]

        raise ValueError(
            f"A profile was requested from a segmented result set without specifying which segment to return: {self._segments}"
        )

    def get_writables(self) -> Optional[List[_Writable]]:
        results: Optional[List[_Writable]] = None
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
                    view = profile.view() if isinstance(profile, DatasetProfile) else profile
                    segmented_profile = SegmentedDatasetProfileView(
                        profile_view=view, segment=segment_key, partition=first_partition
                    )
                    if self.metadata:
                        segmented_profile.metadata.update(self.metadata)
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

    def get_whylabs_tags(self) -> List[SegmentTag]:
        views = self.get_writables()
        if views is None:
            logger.warning("SegmentedResultSet contains no segments")
            return []

        if self._partitions is None:
            logger.warning("SegmentedResultSet contains no partitions.")
            return []

        partitions: List[Any] = self.partitions  # type: ignore
        if len(partitions) > 1:
            logger.warning(
                "SegmentedResultSet contains more than one partition. Only the first partition will be uploaded. "
            )
        partition = partitions[0]
        whylabs_tags = list()
        for view in views:
            view_tags = list()
            if view.partition.id != partition.id:
                continue
            _, segment_tags, _ = _generate_segment_tags_metadata(view.segment, view.partition)
            for segment_tag in segment_tags:
                tag_key = segment_tag.key.replace("whylogs.tag.", "")
                tag_value = segment_tag.value
                view_tags.append({"key": tag_key, "value": tag_value})
            whylabs_tags.append(view_tags)

        return _flatten_tags(whylabs_tags)

    def get_timestamps(self) -> List[Optional[datetime]]:
        views = self.get_writables()
        if views is None:
            logger.warning("SegmentedResultSet contains no segments")
            return []

        times = list()
        for view in views:
            times.append(view.dataset_timestamp)
        return times

    def _get_default_filename(self) -> str:
        return ""  # unused, the segment Wriables are called

    def _write(
        self, path: Optional[str] = None, filename: Optional[str] = None, **kwargs: Any
    ) -> Tuple[bool, Union[str, List[str]]]:
        all_success = True
        files = []
        writables = self.get_writables()  # TODO: support mulit-segment files
        if writables is None:
            raise ValueError("No results to write")
        if len(writables) > 1 and filename is not None:
            raise ValueError("Cannot specify filename for multiple files")

        for writable in writables:
            success, written_files = writable._write(path, filename, **kwargs)
            all_success = all_success and success
            if isinstance(written_files, list):
                files += written_files
            else:
                files.append(written_files)
        return all_success, files

    @property
    def dataset_properties(self) -> Optional[Dict[str, Any]]:
        return self._dataset_properties

    @property
    def dataset_metrics(self) -> Optional[Dict[str, Any]]:
        return self._metrics

    @property
    def partitions(self) -> Optional[List[SegmentationPartition]]:
        return self._partitions

    def set_dataset_timestamp(self, dataset_timestamp: datetime) -> None:
        # TODO: pull dataset_timestamp up into a result set scoped property
        segment_keys = self.segments()
        if not segment_keys:
            return
        for key in segment_keys:
            profile = self.profile(segment=key)
            if profile:
                profile.set_dataset_timestamp(dataset_timestamp)

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

    def segments_in_partition(
        self, partition: SegmentationPartition
    ) -> Optional[Dict[Segment, Union[DatasetProfile, DatasetProfileView]]]:
        return self._segments.get(partition.id)

    def view(self, segment: Optional[Segment] = None) -> Optional[DatasetProfileView]:
        result = self.profile(segment)
        view = result.view() if isinstance(result, DatasetProfile) else result
        return view

    def get_model_performance_metrics_for_segment(self, segment: Segment) -> Optional[ModelPerformanceMetrics]:
        if segment.parent_id in self._segments:
            profile = self._segments[segment.parent_id][segment]
            if not profile:
                logger.warning(
                    f"No profile found for segment {segment} when requesting model performance metrics, returning None!"
                )
                return None

            if isinstance(profile, DatasetProfileView):
                view = profile
            else:
                if hasattr(profile, "view"):
                    view = profile.view()
                else:
                    logger.error(
                        f"Unexpected type: {type(profile)} -> {profile}, cannot check for model performance metrics."
                    )
                    return None
            return view.model_performance_metrics
        return None

    def add_metrics_for_segment(self, metrics: ModelPerformanceMetrics, segment: Segment) -> None:
        if segment.parent_id in self._segments:
            profile = self._segments[segment.parent_id][segment]
            profile.add_model_performance_metrics(metrics)

    @staticmethod
    def zero() -> "SegmentedResultSet":
        return SegmentedResultSet(segments=dict())

    @property
    def model_performance_metric(self) -> Optional[ModelPerformanceMetrics]:
        if self._metrics:
            return self._metrics.get(_MODEL_PERFORMANCE)
        return None

    def add_model_performance_metrics(self, metrics: ModelPerformanceMetrics) -> None:
        if self._metrics:
            self._metrics[_MODEL_PERFORMANCE] = metrics
        else:
            self._metrics = {_MODEL_PERFORMANCE: metrics}

    def add_metric(self, name: str, metric: Metric) -> None:
        if not self._metrics:
            self._metrics = dict()
        self._metrics[name] = metric

    def merge(self, other: "ResultSet") -> "SegmentedResultSet":
        if other is None:
            return self

        if isinstance(other, SegmentedResultSet):
            lhs_partitions: List[SegmentationPartition] = self.partitions or list()
            rhs_partitions: List[SegmentationPartition] = other.partitions or list()

            lhs_segments: Dict[str, Dict[Segment, DatasetProfile]] = self._segments or dict()
            rhs_segments: Dict[str, Dict[Segment, DatasetProfile]] = other._segments or dict()

            merged_segments = _merge_partitioned_segments(lhs_segments, rhs_segments)
            merged_metrics = _merge_metrics(self.dataset_metrics, other.dataset_metrics)
            merged_partitions = _merge_partitions(lhs_partitions, rhs_partitions)
            properties = _accumulate_properties(self._dataset_properties, other.dataset_properties)

            return SegmentedResultSet(merged_segments, merged_partitions, metrics=merged_metrics, properties=properties)
        else:
            raise ValueError(f"Cannot merge incompatible SegmentedResultSet and {type(other)}")


ResultSetWriter = WriterWrapper
