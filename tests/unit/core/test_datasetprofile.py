"""
"""
from uuid import uuid4

from whylabs.logs.core import DatasetProfile
from whylabs.logs.core.datasetprofile import array_profile
from whylabs.logs.util.protobuf import message_to_dict, message_to_json
import json
import datetime
import numpy as np

from whylabs.logs.util.time import to_utc_ms


def test_all_zeros_returns_summary_with_stats():
    stats = ("min", "max", "stddev", "mean")
    array = np.zeros([100, 1])

    prof = array_profile(array)
    msg = prof.to_summary()
    d = message_to_dict(msg)
    d1 = json.loads(message_to_json(msg))
    number_summary = d["columns"]["0"]["numberSummary"]
    missing_stats = [k for k in stats if k not in number_summary]
    if len(missing_stats) > 0:
        raise RuntimeError(f"Stats missing from number summary: {missing_stats}")

    assert d == d1


def test_empty_valid_datasetprofiles_empty():
    now = datetime.datetime.utcnow()
    shared_session_id = uuid4().hex
    x1 = DatasetProfile(
        name="test",
        session_id=shared_session_id,
        session_timestamp=now,
        tags=["tag"],
        metadata={"key": "value"},
    )
    x2 = DatasetProfile(
        name="test",
        session_id=shared_session_id,
        session_timestamp=now,
        tags=["tag"],
        metadata={"key": "value"},
    )

    merged = x1.merge(x2)
    assert merged.name == "test"
    assert merged.session_id == shared_session_id
    assert merged.session_timestamp == now
    assert merged.columns == {}


def test_merge_different_columns():
    now = datetime.datetime.utcnow()
    shared_session_id = uuid4().hex
    x1 = DatasetProfile(
        name="test",
        session_id=shared_session_id,
        session_timestamp=now,
        tags=["tag"],
        metadata={"key": "value"},
    )
    x1.track("col1", "value")
    x2 = DatasetProfile(
        name="test",
        session_id=shared_session_id,
        session_timestamp=now,
        tags=["tag"],
        metadata={"key": "value"},
    )
    x2.track("col2", "value")

    merged = x1.merge(x2)

    assert merged.name == "test"
    assert merged.session_id == shared_session_id
    assert merged.session_timestamp == now
    assert set(list(merged.columns.keys())) == {"col1", "col2"}
    assert merged.columns["col1"].counters.count == 1
    assert merged.columns["col2"].counters.count == 1
    assert len(merged.tags) == 1
    assert "tag" in merged.tags


def test_merge_same_columns():
    now = datetime.datetime.utcnow()
    shared_session_id = uuid4().hex
    x1 = DatasetProfile(
        name="test",
        session_id=shared_session_id,
        session_timestamp=now,
        tags=["tag"],
        metadata={"key": "value"},
    )
    x1.track("col1", "value1")
    x2 = DatasetProfile(
        name="test",
        session_id=shared_session_id,
        session_timestamp=now,
        tags=["tag"],
        metadata={"key": "value"},
    )
    x2.track("col1", "value1")
    x2.track("col2", "value")

    merged = x1.merge(x2)
    assert merged.name == "test"
    assert merged.session_id == shared_session_id
    assert merged.session_timestamp == now
    assert set(list(merged.columns.keys())) == {"col1", "col2"}
    assert merged.columns["col1"].counters.count == 2
    assert merged.columns["col2"].counters.count == 1


def test_protobuf_round_trip():
    now = datetime.datetime.utcnow()
    tags = ("rock", "scissors", "paper")
    original = DatasetProfile(name="test", data_timestamp=now, tags=tags)
    original.track("col1", "value")
    original.track("col2", "value")

    msg = original.to_protobuf()
    roundtrip = DatasetProfile.from_protobuf(msg)

    assert roundtrip.to_protobuf() == msg
    assert roundtrip.name == "test"
    assert roundtrip.session_id == original.session_id
    assert to_utc_ms(roundtrip.session_timestamp) == to_utc_ms(
        original.session_timestamp
    )
    assert set(list(roundtrip.columns.keys())) == {"col1", "col2"}
    assert roundtrip.columns["col1"].counters.count == 1
    assert roundtrip.columns["col2"].counters.count == 1
    assert set(roundtrip.tags) == set(tags)
    assert roundtrip.metadata == original.metadata


def test_non_string_tag_raises_assert_error():
    now = datetime.datetime.utcnow()
    tags = ["tag1", "tag2"]
    x = DatasetProfile("test", now, tags=tags)
    x.validate()
    # Include a non-string tag
    x.tags = tags + [1]
    try:
        x.validate()
        raise RuntimeError("validate should raise an AssertionError")
    except AssertionError:
        pass


def test_non_sorted_tags_raise_value_error():
    now = datetime.datetime.utcnow()
    tags = ["tag1", "tag2"]
    x = DatasetProfile("test", now, tags=tags)
    x.validate()
    # Include a tag which will not be sorted
    x.tags = tags + ["aaaa"]
    try:
        x.validate()
        raise RuntimeError("validate should raise an ValueError")
    except ValueError:
        pass


def test_mismatched_tags_raises_assertion_error():
    now = datetime.datetime.utcnow()
    x1 = DatasetProfile("test", now, tags=["foo"])
    x2 = DatasetProfile("test", now, tags=["bar"])
    try:
        x1.merge(x2)
        raise RuntimeError("Assertion error not raised")
    except AssertionError:
        pass


def test_name_always_appear_in_metadata():
    x1 = DatasetProfile(name="test")
    assert x1.metadata["Name"] == "test"
