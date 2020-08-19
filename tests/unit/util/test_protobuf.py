"""
"""
import json

from whylabs.logs.proto import DoublesMessage
from whylabs.logs.util import protobuf


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
