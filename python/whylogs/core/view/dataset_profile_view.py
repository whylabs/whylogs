import io
import logging
import os
import tempfile
from datetime import datetime, timezone
from enum import Enum
from typing import Any, BinaryIO, Dict, List, Optional, Tuple, Union

from google.protobuf.message import DecodeError

from whylogs.api.writer.writer import _Writable
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
from whylogs.core.stubs import is_not_stub, pd
from whylogs.core.utils import (
    ensure_timezone,
    read_delimited_protobuf,
    write_delimited_protobuf,
)
from whylogs.core.utils.timestamp_calculations import to_utc_milliseconds
from whylogs.core.utils.utils import deprecated
from whylogs.core.view.column_profile_view import ColumnProfileView

# Magic header for whylogs using the first 4 bytes
WHYLOGS_MAGIC_HEADER = "WHY1"
WHYLOGS_MAGIC_HEADER_LEN = 4

WHYLOGS_MAGIC_HEADER_BYTES = WHYLOGS_MAGIC_HEADER.encode("utf-8")
_MODEL_PERFORMANCE = "model_performance_metrics"
_TAG_PREFIX = "whylogs.tag."

logger = logging.getLogger(__name__)


class SummaryType(str, Enum):
    COLUMN = "COLUMN"
    DATASET = "DATASET"


class DatasetProfileView(_Writable):
    _columns: Dict[str, ColumnProfileView]

    def __init__(
        self,
        *,
        columns: Dict[str, ColumnProfileView],
        dataset_timestamp: Optional[datetime],
        creation_timestamp: Optional[datetime],
        metrics: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, str]] = None,
    ):
        if dataset_timestamp:
            ensure_timezone(dataset_timestamp)
        if creation_timestamp:
            ensure_timezone(creation_timestamp)
        self._columns = columns.copy()
        self._dataset_timestamp = dataset_timestamp
        self._creation_timestamp = creation_timestamp
        self._metrics = metrics
        self._metadata: Dict[str, str] = metadata or dict()

    @property
    def dataset_timestamp(self) -> Optional[datetime]:
        return self._dataset_timestamp

    @dataset_timestamp.setter
    def dataset_timestamp(self, date: datetime) -> "DatasetProfileView":
        self._dataset_timestamp = date
        return self

    def set_dataset_timestamp(self, dataset_timestamp: datetime) -> None:
        ensure_timezone(dataset_timestamp)
        self._dataset_timestamp = dataset_timestamp.astimezone(tz=timezone.utc)

    @property
    def creation_timestamp(self) -> Optional[datetime]:
        return self._creation_timestamp

    @property
    def metadata(self) -> Dict[str, str]:
        return self._metadata

    @property
    def model_performance_metrics(self) -> Any:
        if self._metrics:
            return self._metrics.get(_MODEL_PERFORMANCE)
        return None

    @model_performance_metrics.setter
    def model_performance_metrics(self, performance_metrics: Any) -> "DatasetProfileView":
        self.add_model_performance_metrics(performance_metrics)
        return self

    def add_model_performance_metrics(self, metric: Any) -> None:
        if self._metrics:
            self._metrics[_MODEL_PERFORMANCE] = metric
        else:
            self._metrics = {_MODEL_PERFORMANCE: metric}

    @staticmethod
    def _min_datetime(a: Optional[datetime], b: Optional[datetime]) -> Optional[datetime]:
        if not b:
            return a
        if not a:
            return b
        return a if a < b else b

    def _merge_metrics(self, other: "DatasetProfileView") -> Optional[Dict[str, Any]]:
        dataset_level_metrics: Optional[Dict[str, Any]] = None
        if self._metrics:
            if other._metrics:
                dataset_level_metrics = self._metrics
                for metric_name in other._metrics:
                    metric = self._metrics.get(metric_name)
                    if metric:
                        dataset_level_metrics[metric_name] = metric.merge(other._metrics.get(metric_name))
                    else:
                        dataset_level_metrics[metric_name] = other._metrics.get(metric_name)
            else:
                dataset_level_metrics = self._metrics
        else:
            dataset_level_metrics = other._metrics
        return dataset_level_metrics

    def _merge_metadata(self, other: "DatasetProfileView") -> Optional[Dict[str, str]]:
        metadata: Optional[Dict[str, str]] = None
        if self._metadata:
            if other._metadata:
                metadata = self._metadata
                metadata.update(other._metadata)
            else:
                metadata = self._metadata
        else:
            metadata = other._metadata
        return metadata

    def _merge_columns(self, other: "DatasetProfileView") -> Optional[Dict[str, ColumnProfileView]]:
        if self._columns:
            if other._columns:
                all_column_names = set(self._columns.keys()).union(other._columns.keys())
            else:
                all_column_names = set(self._columns.keys())
        else:
            if other._columns:
                all_column_names = set(other._columns.keys())
            else:
                all_column_names = set()
        merged_columns: Dict[str, ColumnProfileView] = {}
        for n in all_column_names:
            lhs = self._columns.get(n)
            rhs = other._columns.get(n)

            res = lhs
            if lhs is None:
                res = rhs
            elif rhs is not None:
                res = lhs + rhs
            assert res is not None
            merged_columns[n] = res
        return merged_columns

    def merge(self, other: "DatasetProfileView") -> "DatasetProfileView":
        merged_columns = self._merge_columns(other)
        dataset_level_metrics = self._merge_metrics(other)
        metadata = self._merge_metadata(other)

        return DatasetProfileView(
            columns=merged_columns or dict(),
            dataset_timestamp=DatasetProfileView._min_datetime(self._dataset_timestamp, other.dataset_timestamp),
            creation_timestamp=DatasetProfileView._min_datetime(self._creation_timestamp, other.creation_timestamp),
            metrics=dataset_level_metrics,
            metadata=metadata,
        )

    def get_column(self, col_name: str) -> Optional[ColumnProfileView]:
        return self._columns.get(col_name)

    def get_columns(self, col_names: Optional[List[str]] = None) -> Dict[str, ColumnProfileView]:
        if col_names:
            return {k: self._columns.get(k) for k in col_names}
        else:
            return {k: self._columns.get(k) for k in self._columns}

    def _get_default_filename(self) -> str:
        return f"profile_{self.creation_timestamp}.bin"

    def _write_it(self, out_f: BinaryIO) -> Tuple[bool, str]:
        if self._metrics and _MODEL_PERFORMANCE in self._metrics:
            from whylogs.migration.converters import v1_to_dataset_profile_message_v0

            message_v0 = v1_to_dataset_profile_message_v0(self, None, None)
            write_delimited_protobuf(out_f, message_v0)
            return True, out_f.name

        self._do_write(out_f)
        return True, out_f.name

    @deprecated(message="please use a Writer")
    def write(self, path: Optional[str] = None, **kwargs: Any) -> Tuple[bool, str]:
        success, files = self._write("", path, **kwargs)
        files = files[0] if isinstance(files, list) else files
        return success, files

    def _write(
        self, path: Optional[str] = None, filename: Optional[str] = None, **kwargs: Any
    ) -> Tuple[bool, Union[str, List[str]]]:
        file_to_write = kwargs.get("file")
        if file_to_write is None:
            path = path if path is not None else self._get_default_path()
            filename = filename if filename is not None else self._get_default_filename()
            path = os.path.join(path, filename) if path is not None else filename
            with _Writable._safe_open_write(path, "+b") as out_f:
                return self._write_it(out_f)

        return self._write_it(file_to_write)

    @staticmethod
    def _split_tags_and_metadata(
        tags: Optional[Dict[str, str]]
    ) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, str]]]:
        # nothing to split, return (None, None)
        # for use in serializing dataset property's tags and metadata
        if not tags:
            return None, None
        message_tags: Optional[Dict[str, str]] = None
        metadata: Dict[str, str] = dict(tags)
        tag_keys: List[str] = list()
        for key, value in metadata.items():
            if key and key.startswith(_TAG_PREFIX):
                if message_tags is None:
                    message_tags = dict()
                message_tags[key] = str(value)
                tag_keys.append(key)
            if not isinstance(value, str):
                metadata[key] = str(value)
        for key in tag_keys:
            metadata.pop(key)
        return message_tags, metadata

    @staticmethod
    def _combine_tags_and_metadata(
        tags: Optional[Dict[str, str]],
        metadata: Optional[Dict[str, str]],
    ) -> Optional[Dict[str, str]]:
        if not tags:
            return dict(metadata) if metadata else None
        if not metadata:
            return dict(tags)
        merged_meta = dict()
        merged_meta.update(tags)
        merged_meta.update(metadata)
        return merged_meta

    def _do_write(self, out_f: BinaryIO) -> Tuple[bool, str]:
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
            tags, metadata = DatasetProfileView._split_tags_and_metadata(self._metadata)

            properties = DatasetProperties(
                dataset_timestamp=to_utc_milliseconds(self._dataset_timestamp),
                creation_timestamp=to_utc_milliseconds(self._creation_timestamp),
                tags=tags,
                metadata=metadata,
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

            out_f.write(WHYLOGS_MAGIC_HEADER_BYTES)
            write_delimited_protobuf(out_f, dataset_segment_header)
            write_delimited_protobuf(out_f, dataset_header)

            f.seek(0)
            while f.tell() < total_len:
                buffer = f.read(1024)
                out_f.write(buffer)
        return True, "Wrote given BinaryIO:" + str(out_f)

    def serialize(self) -> bytes:
        f = io.BytesIO()
        self._do_write(f)
        f.seek(0)
        return f.read()

    @classmethod
    def zero(cls) -> "DatasetProfileView":
        return DatasetProfileView(columns=dict(), dataset_timestamp=None, creation_timestamp=None)

    @classmethod
    def deserialize(cls, data: bytes) -> "DatasetProfileView":
        f = io.BytesIO()
        f.write(data)
        f.seek(0)
        return cls._do_read(f)

    @classmethod
    def read(cls, path: str) -> "DatasetProfileView":
        with open(path, "r+b") as f:
            return cls._do_read(f)

    @classmethod
    def _do_read(cls, f: BinaryIO) -> "DatasetProfileView":
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

        # TODO: support result set reads for multi-segment files
        # when we support serializing multi-segment files
        if dataset_segment_header.has_segments:
            if len(dataset_segment_header.offsets.values()) > 1:
                logger.warning(
                    "File contains more than one segment, but only first profile will be "
                    f"deserialized into this DatasetProfileView: {dataset_segment_header}"
                )

        dataset_profile_header = read_delimited_protobuf(f, DatasetProfileHeader)
        if dataset_profile_header.ByteSize() == 0:
            raise DeserializationError("Missing valid dataset profile header")
        dataset_timestamp = datetime.fromtimestamp(
            dataset_profile_header.properties.dataset_timestamp / 1000.0, tz=timezone.utc
        )
        creation_timestamp = datetime.fromtimestamp(
            dataset_profile_header.properties.creation_timestamp / 1000.0, tz=timezone.utc
        )
        metadata = DatasetProfileView._combine_tags_and_metadata(
            dataset_profile_header.properties.tags, dataset_profile_header.properties.metadata
        )
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
                        f"Expecting chunk header type to be {ChunkHeader.ChunkType.COLUMN}, " f"got {chunk_header.type}"
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
            columns=columns,
            dataset_timestamp=dataset_timestamp,
            creation_timestamp=creation_timestamp,
            metadata=metadata,
        )

    def __getstate__(self) -> bytes:
        return self.serialize()

    def __setstate__(self, state: bytes) -> None:
        copy = DatasetProfileView.deserialize(state)
        self._columns = copy._columns
        self._dataset_timestamp = copy._dataset_timestamp
        self._creation_timestamp = copy._creation_timestamp
        self._metrics = copy._metrics
        self._metadata = copy._metadata

    def _is_uncompounded(self, col_name):
        groups = col_name.split(".")
        metric = groups[0]
        if not len(groups) > 1:
            return False
        if metric in self._columns.keys():
            for metric_name in self._columns[metric].get_metric_component_paths():
                new_metric_name = metric_name.split(":")[0].replace("/", ".")
                if col_name == new_metric_name:
                    return True
        return False

    def to_pandas(self, column_metric: Optional[str] = None, cfg: Optional[SummaryConfig] = None) -> pd.DataFrame:
        all_dicts = []
        if self._columns:
            for col_name, col in sorted(self._columns.items()):
                if not self._is_uncompounded(col_name):
                    sum_dict = col.to_summary_dict(column_metric=column_metric, cfg=cfg)
                    sum_dict["column"] = col_name
                    sum_dict["type"] = SummaryType.COLUMN
                    all_dicts.append(dict(sorted(sum_dict.items())))
            if is_not_stub(pd.DataFrame):
                df = pd.DataFrame(all_dicts)
                return df.set_index("column")
            else:
                raise ImportError("Pandas is not installed. Please install pandas to use this feature.")
        return pd.DataFrame(all_dicts)
