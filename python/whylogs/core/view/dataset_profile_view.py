import logging
import tempfile
from enum import Enum
from typing import Dict, Optional

from google.protobuf.message import DecodeError

from whylogs.core.configs import SummaryConfig
from whylogs.core.errors import DeserializationError
from whylogs.core.proto import (
    ChunkHeader,
    ChunkOffsets,
    ColumnMessage,
    DatasetProfileHeader,
    DatasetProfileMessage,
    MetricComponentMessage,
)
from whylogs.core.stubs import pd
from whylogs.core.utils import read_delimited_protobuf, write_delimited_protobuf
from whylogs.core.view.column_profile_view import ColumnProfileView

logger = logging.getLogger(__name__)


class SummaryType(str, Enum):
    COLUMN = "COLUMN"
    DATASET = "DATASET"


class DatasetProfileView(object):
    _columns: Dict[str, ColumnProfileView]

    def __init__(self, columns: Dict[str, ColumnProfileView]):
        self._columns = columns.copy()

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
        return DatasetProfileView(columns=merged_columns)

    def get_column(self, col_name: str) -> Optional[ColumnProfileView]:
        return self._columns.get(col_name)

    def write(self, path: str) -> None:
        column_chunk_offsets: Dict[str, ChunkOffsets] = {}
        with tempfile.TemporaryFile("w+b") as f:
            for col_name in sorted(self._columns.keys()):
                column_chunk_offsets[col_name] = ChunkOffsets(offsets=[f.tell()])

                col = self._columns[col_name]
                col_msg = col.serialize()

                chunk_header = ChunkHeader(type=ChunkHeader.ChunkType.COLUMN, length=col_msg.ByteSize())
                write_delimited_protobuf(f, chunk_header)
                f.write(col_msg.SerializeToString())

            total_len = f.tell()
            f.flush()

            dataset_header = DatasetProfileHeader(column_offsets=column_chunk_offsets)

            with open(path, "w+b") as out_f:
                write_delimited_protobuf(out_f, dataset_header)

                f.seek(0)
                while f.tell() < total_len:
                    buffer = f.read(1024)
                    out_f.write(buffer)

    @classmethod
    def read(cls, path: str) -> "DatasetProfileView":
        with open(path, "rb") as f:
            header = read_delimited_protobuf(f, DatasetProfileHeader)
            if header is None:
                raise DeserializationError("Missing valid dataset profile header")

            start_offset = f.tell()

            columns = {}
            for col_name in sorted(header.column_offsets.keys()):
                col_offsets = header.column_offsets[col_name]
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

                    col_msg_chunk = ColumnMessage()
                    buf = f.read(chunk_header.length)
                    if len(buf) != chunk_header.length:
                        raise IOError(
                            f"Invalid message for {col_name}. Expecting buffer length of {chunk_header.length}, got {len(buf)}. "
                            f"Offset: {actual_offset}"
                        )
                    try:
                        col_msg_chunk.ParseFromString(buf)
                    except DecodeError:
                        raise DeserializationError(f"Failed to parse protobuf message for column: {col_name}")

                    all_metric_components.update(col_msg_chunk.metric_components)
                column_msg = ColumnMessage(metric_components=all_metric_components)
                columns[col_name] = ColumnProfileView.deserialize(column_msg)
            return DatasetProfileView(columns=columns)

    def to_pandas(self, column_metric: Optional[str] = None, cfg: Optional[SummaryConfig] = None) -> pd.DataFrame:
        all_dicts = []
        for col_name, col in self._columns.items():
            sum_dict = col.to_summary_dict(column_metric=column_metric, cfg=cfg)
            sum_dict["column"] = col_name
            sum_dict["type"] = SummaryType.COLUMN
            all_dicts.append(sum_dict)
        df = pd.DataFrame(all_dicts)
        return df.set_index("column")

    @classmethod
    def from_protobuf(cls, msg: DatasetProfileMessage) -> "DatasetProfileView":
        res = {}
        for col_name, col_msg in msg.columns.items():
            res[col_name] = ColumnProfileView.deserialize(col_msg)

        return DatasetProfileView(columns=res)
