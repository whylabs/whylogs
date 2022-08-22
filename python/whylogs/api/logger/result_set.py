from abc import ABC, abstractmethod
from functools import reduce
from logging import getLogger
from typing import Any, Dict, List, Optional

from whylogs.api.reader import Reader, Readers
from whylogs.api.writer import Writer, Writers
from whylogs.core import DatasetProfile, DatasetProfileView, Segment
from whylogs.core.segmentation_partition import SegmentationPartition

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
        # TODO: multi-profile writer
        view = self._result_set.view()
        self._writer.write(profile=view, **kwargs)


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
    def view(self) -> DatasetProfileView:
        pass

    @abstractmethod
    def profile(self) -> DatasetProfile:
        pass


class ProfileResultSet(ResultSet):
    def __init__(self, profile: DatasetProfile) -> None:
        self._profile = profile

    def profile(self) -> DatasetProfile:
        return self._profile

    def view(self) -> DatasetProfileView:
        return self._profile.view()


class ViewResultSet(ResultSet):
    def __init__(self, view: DatasetProfileView) -> None:
        self._view = view

    def profile(self) -> DatasetProfile:
        raise ValueError("No profile available. Can only view")

    def view(self) -> DatasetProfileView:
        return self._view


class SegmentedResultSet(ResultSet):
    def __init__(
        self,
        segments: Dict[str, Dict[Segment, DatasetProfile]],
        partitions: Optional[List[SegmentationPartition]] = None,
    ) -> None:
        self._segments = segments
        self._partitions = partitions

    def profile(self, segment: Optional[Segment] = None) -> Optional[DatasetProfile]:
        if not self._segments:
            return None
        elif segment:
            paritition_segments = self._segments.get(segment.parent_id)
            return paritition_segments.get(segment) if paritition_segments else None
        elif len(self._segments) == 1:
            for partition_id in self._segments:
                segments = self._segments.get(partition_id)
                number_of_segments = len(segments)
                if number_of_segments == 1:
                    for key in segments:
                        return segments[key]

        raise ValueError(
            f"A profile was requested from a segmented result set without specifying which segment to return: {self._segments}"
        )

    @property
    def partitions(self) -> Optional[List[SegmentationPartition]]:
        return self._partitions

    @property
    def segments(self) -> Optional[List[Segment]]:
        return (
            list(reduce(lambda a, b: list(a.keys()) + list(b.keys()), self._segments.values()))
            if self._segments
            else None
        )

    @property
    def count(self) -> int:
        result = 0
        if self._segments:
            for segment_key in self._segments:
                profiles = self._segments[segment_key]
                result += len(profiles)
        return result

    def segments_in_partition(self, partition: Optional[SegmentationPartition]) -> Optional[List[Segment]]:
        return self._segments.get(partition.id) if partition else None

    def view(self, segment: Optional[Segment]) -> DatasetProfileView:
        return self.profile(segment).view()
