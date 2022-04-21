import os

from google.protobuf.internal.decoder import (
    _DecodeVarint32 as DecoderVarint32,  # type: ignore
)

from whylogs.core.proto.v0 import DatasetProfileMessageV0
from whylogs.migration.converters import v0_to_v1_view

script_dir = os.path.dirname(os.path.realpath(__file__))


def test_convert_v0_to_v1_view() -> None:
    with open(f"{script_dir}/v0_profile.bin", "rb") as f:
        data = f.read()
        msg_len, new_pos = DecoderVarint32(data, 0)
        msg_buf = data[new_pos : new_pos + msg_len]
        prof = DatasetProfileMessageV0.FromString(msg_buf)

    v1_view = v0_to_v1_view(prof)
    df = v1_view.to_pandas()
    assert len(df) > 0
