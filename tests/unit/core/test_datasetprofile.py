"""
"""
from whylabs.logs.core import DatasetProfile
import datetime


def test_empty_valid_datasetprofiles_empty():
    now = datetime.datetime.utcnow()
    x1 = DatasetProfile('test', now)
    x2 = DatasetProfile('test', now)

    merged = x1.merge(x2)
    assert merged.name == 'test'
    assert merged.timestamp == now
    assert merged.columns == {}


def test_merge_different_columns():
    now = datetime.datetime.utcnow()
    x1 = DatasetProfile('test', now, tags=['tag'])
    x1.track('col1', 'value')
    x2 = DatasetProfile('test', now, tags=['tag'])
    x2.track('col2', 'value')

    merged = x1.merge(x2)

    assert merged.name == 'test'
    assert merged.timestamp == now
    assert set(list(merged.columns.keys())) == {'col1', 'col2'}
    assert merged.columns['col1'].counters.count == 1
    assert merged.columns['col2'].counters.count == 1
    assert len(merged.tags) == 1
    assert 'tag' in merged.tags


def test_merge_same_columns():
    now = datetime.datetime.utcnow()
    x1 = DatasetProfile('test', now)
    x1.track('col1', 'value1')
    x2 = DatasetProfile('test', now)
    x2.track('col1', 'value1')
    x2.track('col2', 'value')

    merged = x1.merge(x2)
    assert merged.name == 'test'
    assert merged.timestamp == now
    assert set(list(merged.columns.keys())) == {'col1', 'col2'}
    assert merged.columns['col1'].counters.count == 2
    assert merged.columns['col2'].counters.count == 1


def test_protobuf_round_trip():
    now = datetime.datetime.utcnow()
    tags = ('rock', 'scissors', 'paper')
    x = DatasetProfile("test", now, tags=tags)
    x.track('col1', 'value')
    x.track('col2', 'value')

    msg = x.to_protobuf()
    roundtrip = DatasetProfile.from_protobuf(msg)

    assert roundtrip.to_protobuf() == msg
    assert roundtrip.name == 'test'
    assert set(list(roundtrip.columns.keys())) == {'col1', 'col2'}
    assert roundtrip.columns['col1'].counters.count == 1
    assert roundtrip.columns['col2'].counters.count == 1
    assert set(roundtrip.tags) == set(tags)


def test_non_string_tag_raises_assert_error():
    now = datetime.datetime.utcnow()
    tags = ['tag1', 'tag2']
    x = DatasetProfile('test', now, tags=tags)
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
    tags = ['tag1', 'tag2']
    x = DatasetProfile('test', now, tags=tags)
    x.validate()
    # Include a tag which will not be sorted
    x.tags = tags + ['aaaa']
    try:
        x.validate()
        raise RuntimeError("validate should raise an ValueError")
    except ValueError:
        pass


def test_mismatched_tags_raises_assertion_error():
    now = datetime.datetime.utcnow()
    x1 = DatasetProfile('test', now, tags=['foo'])
    x2 = DatasetProfile('test', now, tags=['bar'])
    try:
        x1.merge(x2)
        raise RuntimeError('Assertion error not raised')
    except AssertionError:
        pass
