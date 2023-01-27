"""
A read/write library for length-delimited protobuf messages.

Based on: https://github.com/soulmachine/delimited-protobuf/blob/main/delimited_protobuf.py"""
import warnings
from logging import getLogger
from typing import IO, Type, TypeVar

from google.protobuf.message import Message

from whylogs.core.errors import DeserializationError

T = TypeVar("T", bound=Message)
logger = getLogger(__name__)
_DECODE_ERROR_STRING = "Unexpected end-group tag"


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
    delimited_read_failure = False

    with warnings.catch_warnings(record=True) as warning_messages:
        try:
            logger.info(f"Attempting to read profile file as delimited from position {offset}.")
            msg.ParseFromString(buf)
        except Exception as decode_error:
            logger.warning(
                f"{decode_error}: when reading delimited protobuf file of size "
                f"{size} bytes as {proto_class_name}, falling back to non-delimited read."
            )
            delimited_read_failure = True

        if not delimited_read_failure and len(warning_messages) > 0:
            for w in warning_messages:
                if issubclass(w.category, RuntimeWarning):
                    logger.info(
                        f"Encountered RuntimeWarning when reading delimited file: {w.message}, retrying to read profile as non-delimited file."
                    )
                    if _DECODE_ERROR_STRING in str(w.message):
                        delimited_read_failure = True
                        break
            if not delimited_read_failure:
                logger.info(
                    f"Encountered {len(warning_messages)} warnings, e.g. {warning_messages[-1].message} but no DecodeError message detected, "
                    "proceeding with the already delimited parsed file."
                )

    if delimited_read_failure:
        logger.info("Attempting to read profile file as non-delimited from the beggining of the file.")
        stream.seek(0)
        buf = stream.read()
        try:
            msg.ParseFromString(buf)
            logger.info("successfully read profile file as non-delimited.")
        except Exception as e:
            logger.error(f"{e}: Occured during attempted fallback read of {proto_class_name}.")
            raise DeserializationError(f"Failed fallback attempt to read {proto_class_name}:{e}")

    return msg


def write_delimited_protobuf(stream: IO[bytes], msg: T) -> None:
    """Write a single length-delimited message to the given stream."""
    assert stream is not None
    from google.protobuf.internal.encoder import _EncodeVarint  # type: ignore

    _EncodeVarint(stream.write, msg.ByteSize())
    stream.write(msg.SerializeToString())
