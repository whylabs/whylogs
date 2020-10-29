import datetime
import json
import os
from uuid import uuid4

import numpy as np

from whylogs.core.datasetprofile import DatasetProfile, array_profile
from whylogs.util import time
from whylogs.util.protobuf import message_to_dict, message_to_json
from whylogs.util.time import to_utc_ms


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
        tags={"key": "value"},
        metadata={"key": "value"},
    )
    x2 = DatasetProfile(
        name="test",
        session_id=shared_session_id,
        session_timestamp=now,
        tags={"key": "value"},
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
        tags={"key": "value"},
        metadata={"key": "x1"},
    )
    x1.track("col1", "value")
    x2 = DatasetProfile(
        name="test",
        session_id=shared_session_id,
        session_timestamp=now,
        tags={"key": "value"},
        metadata={"key": "x2"},
    )
    x2.track("col2", "value")

    merged = x1.merge(x2)

    assert merged.name == "test"
    assert merged.session_id == shared_session_id
    assert merged.session_timestamp == now
    assert set(list(merged.columns.keys())) == {"col1", "col2"}
    assert merged.columns["col1"].counters.count == 1
    assert merged.columns["col2"].counters.count == 1
    assert merged.tags == dict({"Name": "test", "key": "value"})
    assert merged.metadata == dict({"key": "x1"})


def test_merge_same_columns():
    now = datetime.datetime.utcnow()
    shared_session_id = uuid4().hex
    x1 = DatasetProfile(
        name="test",
        session_id=shared_session_id,
        session_timestamp=now,
        tags={"key": "value"},
        metadata={"key": "value"},
    )
    x1.track("col1", "value1")
    x2 = DatasetProfile(
        name="test",
        session_id=shared_session_id,
        session_timestamp=now,
        tags={"key": "value"},
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
    tags = {"k1": "rock", "k2": "scissors", "k3": "paper"}
    original = DatasetProfile(name="test", dataset_timestamp=now, tags=tags,)
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

    tags["Name"] = "test"
    assert set(roundtrip.tags) == set(tags)
    assert roundtrip.metadata == original.metadata


def test_non_string_tag_raises_assert_error():
    now = datetime.datetime.utcnow()
    tags = {"key": "value"}
    x = DatasetProfile("test", now, tags=tags)
    x.validate()
    # Include a non-string tag
    x._tags["number"] = 1

    try:
        x.validate()
        raise RuntimeError("validate should raise an AssertionError")
    except AssertionError:
        pass


def test_mismatched_tags_raises_assertion_error():
    now = datetime.datetime.utcnow()
    x1 = DatasetProfile("test", now, tags={"key": "foo"})
    x2 = DatasetProfile("test", now, tags={"key": "bar"})
    try:
        x1.merge_strict(x2)
        raise RuntimeError("Assertion error not raised")
    except AssertionError:
        pass


def test_mismatched_tags_merge_succeeds():
    now = datetime.datetime.utcnow()
    x1 = DatasetProfile("test", now, tags={"key": "foo"})
    x2 = DatasetProfile("test2", now, tags={"key": "bar"})

    result = x1.merge(x2)
    assert result.tags.get("key") == "foo"


def test_name_always_appear_in_tags():
    x1 = DatasetProfile(name="test")
    assert x1.tags["Name"] == "test"


def test_parse_delimited_from_java_single():
    dir_path = os.path.dirname(os.path.realpath(__file__))

    with open(os.path.join(dir_path, "output_from_java_08242020.bin"), "rb") as f:
        data = f.read()
        assert DatasetProfile.parse_delimited_single(data) is not None


def test_parse_delimited_from_java_multiple():
    dir_path = os.path.dirname(os.path.realpath(__file__))

    with open(os.path.join(dir_path, "output_from_java_08242020.bin"), "rb") as f:
        data = f.read()
        multiple = data + data
        result = DatasetProfile.parse_delimited(multiple)
        assert len(result) == 2


def test_write_delimited_single():
    now = datetime.datetime.utcnow()

    original = DatasetProfile(
        name="test",
        session_id="test.session.id",
        session_timestamp=now,
        tags={"key": "value"},
        metadata={"key": "value"},
    )
    original.track("col1", "value")

    output_bytes = original.serialize_delimited()
    pos, roundtrip = DatasetProfile.parse_delimited_single(output_bytes)

    assert roundtrip.session_id == original.session_id
    # Python time precision includes nanoseconds
    assert time.to_utc_ms(roundtrip.session_timestamp) == time.to_utc_ms(
        original.session_timestamp
    )
    assert roundtrip.tags == original.tags
    assert roundtrip.metadata == original.metadata


def test_write_delimited_multiple():
    now = datetime.datetime.utcnow()

    original = DatasetProfile(
        name="test",
        session_id="test.session.id",
        session_timestamp=now,
        tags={"key": "value"},
        metadata={"key": "value"},
    )
    original.track("col1", "value")

    output_bytes = original.serialize_delimited()

    multiple_entries = output_bytes
    for i in range(1, 5):
        multiple_entries += output_bytes

    entries = DatasetProfile.parse_delimited(multiple_entries)
    assert len(entries) == 5

    for entry in entries:
        assert entry.session_id == original.session_id
        # Python time precisions are different
        assert time.to_utc_ms(entry.session_timestamp) == time.to_utc_ms(
            original.session_timestamp
        )
        assert entry.tags == original.tags
        assert entry.metadata == original.metadata


def test_verify_schema_version():
    dp = DatasetProfile(
        name="test",
        session_id="test.session.id",
        session_timestamp=datetime.datetime.now(),
        tags={"key": "value"},
        metadata={"key": "value"},
    )
    props = dp.to_properties()
    assert props.schema_major_version == 1
    assert props.schema_minor_version == 1
