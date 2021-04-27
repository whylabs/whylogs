import json
import os

from whylogs.proto import DoublesMessage, DatasetProperties, NumbersMessage
from whylogs.util import protobuf

_MY_DIR = os.path.realpath(os.path.dirname(__file__))


def test_message_to_dict_returns_default_values():
    msg1 = DoublesMessage(min=0, max=0, sum=0, count=10)
    d1 = protobuf.message_to_dict(msg1)

    msg2 = DoublesMessage(count=10)
    d2 = protobuf.message_to_dict(msg2)

    true_val = {
        "min": 0.0,
        "max": 0.0,
        "sum": 0.0,
        "count": "10",
    }
    assert d1 == true_val
    assert d2 == true_val


def test_message_to_dict_equals_message_to_json():
    msg = DoublesMessage(min=0, max=1.0, sum=2.0, count=10)
    d1 = protobuf.message_to_dict(msg)
    d2 = json.loads(protobuf.message_to_json(msg))
    assert d1 == d2


def test_message_to_dict():
    msg1 = DoublesMessage(min=0, max=0, sum=0, count=10)
    protobuf.repr_message(msg1)
