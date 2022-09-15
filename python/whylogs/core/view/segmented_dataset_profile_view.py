import tempfile
from logging import getLogger
from typing import Any, Dict, Optional

from whylogs.api.writer.writer import Writable
from whylogs.core.proto import (
    ChunkHeader,
    ChunkMessage,
    ChunkOffsets,
    DatasetProfileHeader,
    DatasetProperties,
    DatasetSegmentHeader,
    MetricComponentMessage,
)
from whylogs.core.proto import Segment as SegmentMessage
from whylogs.core.segment import Segment
from whylogs.core.segmentation_partition import SegmentationPartition
from whylogs.core.utils import write_delimited_protobuf
from whylogs.core.utils.timestamp_calculations import to_utc_milliseconds
from whylogs.core.view.dataset_profile_view import (
    WHYLOGS_MAGIC_HEADER_BYTES,
    DatasetProfileView,
)
from whylogs.migration.converters import (
    _generate_segment_tags_metadata,
    v1_to_dataset_profile_message_v0,
)

logger = getLogger(__name__)

_BUFFER_CHUNK = 1024


class SegmentedDatasetProfileView(Writable):
    _profile_view: DatasetProfileView
    _segment: Segment
    _partition: SegmentationPartition

    def __init__(
        self,
        *,
        profile_view: DatasetProfileView,
        segment: Segment,
        partition: SegmentationPartition,
    ):
        self._profile_view = profile_view
        self._segment = segment
        self._partition = partition

    @property
    def segment(self) -> Segment:
        return self._segment

    @property
    def partition(self) -> SegmentationPartition:
        return self._partition

    @property
    def profile_view(self) -> DatasetProfileView:
        return self._profile_view

    @property
    def dataset_timestamp(self):
        return self.profile_view.dataset_timestamp

    @property
    def creation_timestamp(self):
        return self.profile_view.creation_timestamp

    def get_default_path(self) -> str:
        return f"profile_{self._profile_view.creation_timestamp}_{self._segment.parent_id}_{'_'.join(self._segment.key)}.bin"

    def _write_as_v0_message(self, path: Optional[str] = None, **kwargs: Any) -> None:
        message_v0 = v1_to_dataset_profile_message_v0(self.profile_view, self.segment, self.partition)
        path = path or self.get_default_path()
        with open(path, "w+b") as out_f:
            write_delimited_protobuf(out_f, message_v0)

    def _write_v1(self, path: Optional[str] = None, **kwargs: Any) -> None:
        all_metric_component_names = set()
        path = path or self.get_default_path()

        # capture the list of all metric component paths
        for col in self.profile_view._columns.values():
            all_metric_component_names.update(col.get_metric_component_paths())
        metric_name_list = list(all_metric_component_names)
        metric_name_list.sort()
        metric_name_indices: Dict[str, int] = {}
        metric_index_to_name: Dict[int, str] = {}
        for i in range(0, len(metric_name_list)):
            metric_name_indices[metric_name_list[i]] = i
            metric_index_to_name[i] = metric_name_list[i]

        column_chunk_offsets: Dict[str, ChunkOffsets] = {}
        with tempfile.TemporaryFile("w+b") as f:
            for col_name in sorted(self.profile_view._columns.keys()):
                column_chunk_offsets[col_name] = ChunkOffsets(offsets=[f.tell()])

                col = self.profile_view._columns[col_name]

                # for a given column, turn it into a ChunkMessage.
                indexed_component_messages: Dict[int, MetricComponentMessage] = {}
                metric_components = col.to_protobuf().metric_components
                for m_name, m_component in metric_components.items():
                    index = metric_name_indices.get(m_name)
                    if index is None:
                        raise ValueError(f"Missing metric from index mapping. Metric name: {m_name}")
                    indexed_component_messages[index] = m_component

                chunk_msg = ChunkMessage(metric_components=indexed_component_messages)
                chunk_header = ChunkHeader(type=ChunkHeader.ChunkType.COLUMN, length=chunk_msg.ByteSize())
                write_delimited_protobuf(f, chunk_header)
                f.write(chunk_msg.SerializeToString())

            total_len = f.tell()
            f.flush()

            # calculate segment tags based on columnar segments
            segment_message_tags, segment_tags, segment_metadata = _generate_segment_tags_metadata(
                self.segment, self.partition
            )

            if not segment_tags:
                raise NotImplementedError(
                    f"Serialization of segments requires segment tags but none calculated for partition: {self.partition} and segments: {self.segment}"
                )

            segment_message = SegmentMessage()
            segment_message.tags.extend(segment_tags)
            segments_message_field = [segment_message]

            properties = DatasetProperties(
                dataset_timestamp=to_utc_milliseconds(self.profile_view._dataset_timestamp),
                creation_timestamp=to_utc_milliseconds(self.profile_view._creation_timestamp),
                metadata=segment_metadata,
                tags=segment_message_tags,
            )

            logger.warn(f"constructed DatasetProperties for segmented profile file: {properties}")

            dataset_header = DatasetProfileHeader(
                column_offsets=column_chunk_offsets,
                properties=properties,
                length=total_len,
                indexed_metric_paths=metric_index_to_name,
            )

            # TODO: multi segment file format requires multiple offset calculations.
            #  Single segment file only supported initially.
            #  This creates the offsets dictionary with correct keys but 0 offset values, later update offsets
            #  based on file position offset in the temp file written, similarly to how Chunk
            segment_offsets: Dict[int, int] = {
                n: 0 for n in range(len(segments_message_field))
            }  # update this after writing

            # single file segments.
            dataset_segment_header = DatasetSegmentHeader(has_segments=True, offsets=segment_offsets)

            # TODO: calculate other segments offsets when we support multiple segments per file
            write_delimited_protobuf(f, dataset_segment_header)
            first_segment_offset = f.tell() - total_len
            dataset_segment_header.offsets[0] = first_segment_offset

            # only single segment files at first.
            dataset_segment_header.segments.extend(segments_message_field)

            with open(path, "w+b") as out_f:
                out_f.write(WHYLOGS_MAGIC_HEADER_BYTES)
                write_delimited_protobuf(out_f, dataset_segment_header)
                logger.debug(
                    f"Writing segmented profile file: whylogs file and segment headers wrote {out_f.tell()} bytes"
                )
                write_delimited_protobuf(out_f, dataset_header)
                logger.debug(
                    f"Writing segmented profile file: and with dataset header wrote a total of {out_f.tell()} bytes before writing the chunks."
                )

                f.seek(0)
                while f.tell() < total_len:
                    buffer = f.read(_BUFFER_CHUNK)
                    out_f.write(buffer)
                logger.debug(f"Writing segmented profile file: complete! total of {out_f.tell()} bytes written.")

    def write(self, path: Optional[str] = None, **kwargs: Any) -> None:
        if kwargs.get("use_v0"):
            self._write_as_v0_message(path, **kwargs)
        else:
            self._write_v1(path, **kwargs)
