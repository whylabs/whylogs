"""
Functions for interacting with protobuf
"""
import google.protobuf.message
from google.protobuf.json_format import MessageToDict, MessageToJson

from whylogs.util import varint


def message_to_json(x: google.protobuf.message, **kwargs):
    """
    A wrapper for `google.protobuf.json_format.MessageToJson`

    Currently a very thin wrapper...x and kwargs are just passed to
    `MessageToJson`
    """
    return MessageToJson(x, including_default_value_fields=True, **kwargs)


def message_to_dict(x: google.protobuf.message):
    """
    Convert a protobuf message to a dictionary

    A thin wrapper around the google built-in function.
    """
    return MessageToDict(x, including_default_value_fields=True)


def _varint_delim_reader(fp):
    msg_bytes = varint.decode_stream(fp)
    while msg_bytes is not None:
        if msg_bytes <= 0:
            raise RuntimeError("Invalid message size: {}".format(msg_bytes))
        msg_raw = fp.read(msg_bytes)
        yield msg_raw
        msg_bytes = varint.decode_stream(fp)


def _varint_delim_iterator(f):
    """
    Return an iterator to read delimited protobuf messages.  The iterator will
    return protobuf messages one by one as raw `bytes` objects.
    """
    if isinstance(f, str):
        with open(f, "rb") as fp:
            for msg in _varint_delim_reader(fp):
                yield msg
    else:
        for msg in _varint_delim_reader(f):
            yield msg


def multi_msg_reader(f, msg_class):
    """
    Return an iterator to iterate through protobuf messages in a multi-message
    protobuf file.

    See also: `write_multi_msg()`

    Parameters
    ----------
    f : str, file-object
        Filename or open file object to read from
    msg_class : class
        The Protobuf message class, gets instantiated with a call to
        `msg_class()`

    Returns
    -------
    msg_iterator
        Iterator which returns protobuf messages
    """
    for raw_msg in _varint_delim_iterator(f):
        msg = msg_class()
        msg = msg.FromString(raw_msg)
        yield msg


def read_multi_msg(f, msg_class):
    """
    Wrapper for :func:`multi_msg_reader` which reads all the messages and
    returns them as a list.
    """
    return [x for x in multi_msg_reader(f, msg_class)]


def _encode_one_msg(msg: google.protobuf.message):
    n = msg.ByteSize()
    return varint.encode(n) + msg.SerializeToString()


def _write_multi_msg(msgs: list, fp):
    for msg in msgs:
        fp.write(_encode_one_msg(msg))


def write_multi_msg(msgs: list, f):
    """
    Write a list (or iterator) of protobuf messages to a file.

    The multi-message file format is a binary format with:

        <varint MessageBytesSize><message>

    Which is repeated, where the len(message) in bytes is `MessageBytesSize`

    Parameters
    ----------
    msgs : list, iterable
        Protobuf messages to write to disk
    f : str, file-object
        Filename or open binary file object to write to
    """
    if isinstance(f, str):
        with open(f, "wb") as fp:
            _write_multi_msg(msgs, fp)
    else:
        # Assume we have an already open file
        _write_multi_msg(msgs, f)


def repr_message(x: google.protobuf.message.Message, indent=2, display=True):
    """
    Print or generate string preview of a protobuf message.  This is mainly
    to get a preview of the attribute names and structure of a protobuf
    message class.

    Parameters
    ----------
    x : google.protobuf.message.Message
        Message to preview
    indent : int
        Indentation
    display : bool
        If True, print the message and return `None`.  Else, return a string.

    Returns
    -------
    msg : str, None
        If `display == False`, return the message, else return None.
    """
    return _repr_message(x, indent=indent, display=display)


def _repr_message(x, level=0, msg="", display=True, indent=2):

    if hasattr(x, "DESCRIPTOR"):
        # FIXME: this since the metadata is nested now
        field_names = sorted([f.name for f in x.DESCRIPTOR.fields])
        for f in field_names:
            msg = msg + " " * level + str(f) + "\n"
            v = getattr(x, f, None)
            msg = _repr_message(v, level + indent, msg)
    else:
        msg = msg[0:-1] + ": " + str(x)[0:100] + "\n"
    if display and level == 0:
        print(msg)
    else:
        return msg
