import logging
import tempfile
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from google.protobuf.message import DecodeError

from whylogs.api.writer.writer import Writable
from whylogs.core.configs import SummaryConfig
from whylogs.core.errors import DeserializationError
from whylogs.core.proto import (
    ChunkHeader,
    ChunkMessage,
    ChunkOffsets,
    ColumnMessage,
    DatasetProfileHeader,
    DatasetProperties,
    DatasetSegmentHeader,
    MetricComponentMessage,
)
from whylogs.core.stubs import pd
from whylogs.core.utils import read_delimited_protobuf, write_delimited_protobuf
from whylogs.core.utils.timestamp_calculations import to_utc_milliseconds
from whylogs.core.view.column_profile_view import ColumnProfileView

# Magic header for whylogs using the first 4 bytes
WHYLOGS_MAGIC_HEADER = "WHY1"
WHYLOGS_MAGIC_HEADER_LEN = 4

WHYLOGS_MAGIC_HEADER_BYTES = WHYLOGS_MAGIC_HEADER.encode("utf-8")
_MODEL_PERFORMANCE = "model_performance_metrics"

logger = logging.getLogger(__name__)


class SummaryType(str, Enum):
    COLUMN = "COLUMN"
    DATASET = "DATASET"


class DatasetProfileView(Writable):
    _columns: Dict[str, ColumnProfileView]

    def __init__(
        self,
        *,
        columns: Dict[str, ColumnProfileView],
        dataset_timestamp: datetime,
        creation_timestamp: datetime,
        metrics: Optional[Dict[str, Any]] = None,
    ):
        self._columns = columns.copy()
        self._dataset_timestamp = dataset_timestamp
        self._creation_timestamp = creation_timestamp
        self._metrics = metrics

    @property
    def dataset_timestamp(self) -> datetime:
        return self._dataset_timestamp

    @property
    def creation_timestamp(self) -> datetime:
        return self._creation_timestamp

    @property
    def model_performance_metrics(self) -> Any:
        if self._metrics:
            return self._metrics.get(_MODEL_PERFORMANCE)
        return None

    def merge(self, other: "DatasetProfileView") -> "DatasetProfileView":
        all_names = set(self._columns.keys()).union(other._columns.keys())
        merged_columns: Dict[str, ColumnProfileView] = {}
        for n in all_names:
            lhs = self._columns.get(n)
            rhs = other._columns.get(n)

            res = lhs
            if lhs is None:
                res = rhs
            elif rhs is not None:
                res = lhs + rhs
            assert res is not None
            merged_columns[n] = res
        return DatasetProfileView(
            columns=merged_columns,
            dataset_timestamp=self._dataset_timestamp if self._dataset_timestamp else other.dataset_timestamp,
            creation_timestamp=self._creation_timestamp if self._creation_timestamp else other.creation_timestamp,
        )

    def get_column(self, col_name: str) -> Optional[ColumnProfileView]:
        return self._columns.get(col_name)

    def get_columns(self, col_names: Optional[List[str]] = None) -> Dict[str, ColumnProfileView]:
        if col_names:
            return {k: self._columns.get(k) for k in col_names}
        else:
            return {k: self._columns.get(k) for k in self._columns}

    def get_default_path(self) -> str:
        return f"profile_{self.creation_timestamp}.bin"

    def write(self, path: Optional[str] = None, **kwargs: Any) -> None:
        path = path or self.get_default_path()
        if self._metrics and _MODEL_PERFORMANCE in self._metrics:
            from whylogs.migration.converters import v1_to_dataset_profile_message_v0

            message_v0 = v1_to_dataset_profile_message_v0(self, None, None)
            with open(path, "w+b") as out_f:
                write_delimited_protobuf(out_f, message_v0)

            return

        all_metric_component_names = set()

        # capture the list of all metric component paths
        for col in self._columns.values():
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
            for col_name in sorted(self._columns.keys()):
                column_chunk_offsets[col_name] = ChunkOffsets(offsets=[f.tell()])

                col = self._columns[col_name]

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

            properties = DatasetProperties(
                dataset_timestamp=to_utc_milliseconds(self._dataset_timestamp),
                creation_timestamp=to_utc_milliseconds(self._creation_timestamp),
            )
            dataset_header = DatasetProfileHeader(
                column_offsets=column_chunk_offsets,
                properties=properties,
                length=total_len,
                indexed_metric_paths=metric_index_to_name,
            )

            # single file segments
            dataset_segment_header = DatasetSegmentHeader(
                has_segments=False,
            )

            with open(path, "w+b") as out_f:
                out_f.write(WHYLOGS_MAGIC_HEADER_BYTES)
                write_delimited_protobuf(out_f, dataset_segment_header)
                write_delimited_protobuf(out_f, dataset_header)

                f.seek(0)
                while f.tell() < total_len:
                    buffer = f.read(1024)
                    out_f.write(buffer)

    @classmethod
    def read(cls, path: str) -> "DatasetProfileView":
        with open(path, "r+b") as f:
            buf = f.read(WHYLOGS_MAGIC_HEADER_LEN)
            try:
                decoded_header = buf.decode("utf-8")
            except UnicodeDecodeError as e:
                raise DeserializationError("Invalid magic header. Decoder error: %s", e)

            if WHYLOGS_MAGIC_HEADER != decoded_header:
                raise DeserializationError(
                    f"Invalid magic header. Got: {decoded_header} but expecting: {WHYLOGS_MAGIC_HEADER}"
                )

            dataset_segment_header = read_delimited_protobuf(f, DatasetSegmentHeader)
            if dataset_segment_header.has_segments:
                logger.warning(
                    "File contains segments. Only first profile will be deserialized into this DatasetProfileView"
                )

            dataset_profile_header = read_delimited_protobuf(f, DatasetProfileHeader)
            if dataset_profile_header.ByteSize() == 0:
                raise DeserializationError("Missing valid dataset profile header")
            dataset_timestamp = datetime.fromtimestamp(dataset_profile_header.properties.dataset_timestamp / 1000.0)
            creation_timestamp = datetime.fromtimestamp(dataset_profile_header.properties.creation_timestamp / 1000.0)
            indexed_metric_paths = dataset_profile_header.indexed_metric_paths
            if len(indexed_metric_paths) < 1:
                logger.warning("Name index in the header is empty. Possible data corruption")

            start_offset = f.tell()

            columns = {}
            for col_name in sorted(dataset_profile_header.column_offsets.keys()):
                col_offsets = dataset_profile_header.column_offsets[col_name]
                all_metric_components: Dict[str, MetricComponentMessage] = {}
                for offset in col_offsets.offsets:
                    actual_offset = offset + start_offset
                    chunk_header = read_delimited_protobuf(f, proto_class_name=ChunkHeader, offset=actual_offset)
                    if chunk_header is None:
                        raise DeserializationError(
                            f"Missing or corrupt chunk header for column {col_name}. Offset: {actual_offset}"
                        )
                    if chunk_header.type != ChunkHeader.ChunkType.COLUMN:
                        raise DeserializationError(
                            f"Expecting chunk header type to be {ChunkHeader.ChunkType.COLUMN}, "
                            f"got {chunk_header.type}"
                        )

                    chunk_msg = ChunkMessage()
                    buf = f.read(chunk_header.length)
                    if len(buf) != chunk_header.length:
                        raise IOError(
                            f"Invalid message for {col_name}. Expecting buffer length of {chunk_header.length}, "
                            f"got {len(buf)}. "
                            f"Offset: {actual_offset}"
                        )
                    try:
                        chunk_msg.ParseFromString(buf)
                    except DecodeError:
                        raise DeserializationError(f"Failed to parse protobuf message for column: {col_name}")

                    for idx, metric_component in chunk_msg.metric_components.items():
                        full_name = indexed_metric_paths.get(idx)
                        if full_name is None:
                            raise ValueError(f"Missing metric name in the header. Index: {idx}")
                        all_metric_components[full_name] = metric_component

                column_msg = ColumnMessage(metric_components=all_metric_components)
                columns[col_name] = ColumnProfileView.from_protobuf(column_msg)
            return DatasetProfileView(
                columns=columns, dataset_timestamp=dataset_timestamp, creation_timestamp=creation_timestamp
            )

    def to_pandas(self, column_metric: Optional[str] = None, cfg: Optional[SummaryConfig] = None) -> pd.DataFrame:
        all_dicts = []
        for col_name, col in sorted(self._columns.items()):
            sum_dict = col.to_summary_dict(column_metric=column_metric, cfg=cfg)
            sum_dict["column"] = col_name
            sum_dict["type"] = SummaryType.COLUMN
            all_dicts.append(dict(sorted(sum_dict.items())))
        df = pd.DataFrame(all_dicts)
        return df.set_index("column")
