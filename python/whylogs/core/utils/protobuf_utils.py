"""
A read/write library for length-delimited protobuf messages.

Based on: https://github.com/soulmachine/delimited-protobuf/blob/main/delimited_protobuf.py"""
from typing import IO, Type, TypeVar

from google.protobuf.message import Message

T = TypeVar("T", bound=Message)


def _read_varint(stream: IO[bytes], offset: int = 0) -> int:
    from google.protobuf.internal.decoder import _DecodeVarint  # type: ignore

    """Read a varint from the stream."""
    if offset > 0:
        stream.seek(offset)
    buf: bytes = stream.read(1)
    if buf == b"":
        return 0  # reached EOF
    while (buf[-1] & 0x80) >> 7 == 1:  # while the MSB is 1
        new_byte = stream.read(1)
        if new_byte == b"":
            raise EOFError("unexpected EOF")
        buf += new_byte
    varint, _ = _DecodeVarint(buf, 0)
    return varint


def read_delimited_protobuf(stream: IO[bytes], proto_class_name: Type[T], offset: int = 0) -> T:
    """Read a single length-delimited message from the given stream."""
    size = _read_varint(stream, offset=offset)
    if size == 0:
        return proto_class_name()
    buf = stream.read(size)
    msg = proto_class_name()
    msg.ParseFromString(buf)
    return msg


def write_delimited_protobuf(stream: IO[bytes], msg: T) -> None:
    """Write a single length-delimited message to the given stream."""
    assert stream is not None
    from google.protobuf.internal.encoder import _EncodeVarint  # type: ignore

    _EncodeVarint(stream.write, msg.ByteSize())
    stream.write(msg.SerializeToString())
