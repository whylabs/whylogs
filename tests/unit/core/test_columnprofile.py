from whylabs.logs.core import ColumnProfile


def test_track():
    c = ColumnProfile('col')
    data = [1, 2, 3, 'string 1', 'string 2', '3', 4.0, '3.95', '3.95st', None, True]
    for val in data:
        c.track(val)
    nt = c.number_tracker
    assert nt.floats.count == 6
    assert nt.ints.count == 0
    assert nt.floats.min == 1.0
    assert nt.floats.max == 4.0

    assert c.counters.count == len(data)
    assert c.counters.null_count == 1
    assert c.counters.true_count == 1


def test_protobuf():
    c = ColumnProfile('col')
    for val in [1, 2, 3]:
        c.track(val)
    msg = c.to_protobuf()
    c1 = ColumnProfile.from_protobuf(msg)
    assert c1.column_name == c.column_name == 'col'
    assert hasattr(c1, 'number_tracker')
    assert msg == c1.to_protobuf()


def test_summary():
    from whylabs.logs.util.protobuf import message_to_dict
    c = ColumnProfile('col')
    for n in [1, 2, 3]:
        c.track(n)
    summary = c.to_summary()
    summary_dict = message_to_dict(summary)
    true_val = {
        "counters": {
            "count": "3",
        },
        "schema": {
            "inferredType": {
                "type": "INTEGRAL",
                "ratio": 1.0
            },
            "typeCounts": {
                "INTEGRAL": "3"
            }
        },
        "numberSummary": {
            "count": "3",
            "min": 1.0,
            "max": 3.0,
            "mean": 2.0,
            "stddev": 1.0,
            "histogram": {
                "start": 1.0,
                "end": 3.0000003,
                "counts": [
                    "3"
                ],
                "max": 3.0,
                "min": 1.0,
                "bins": [
                    1.0,
                    3.0000003
                ],
                "n": "3",
                "width": 0.0
            },
            "uniqueCount": {
                "estimate": 3.0,
                "upper": 3.0,
                "lower": 3.0
            }
        }
    }
    assert summary_dict == true_val


def test_merge():
    col = ColumnProfile("test")
    vals = [1, 1.0, "string", True, False, None]
    for v in vals:
        col.track(v)

    merged = col.merge(col)
    assert merged.counters.count == 12
    assert merged.counters.null_count == 2
    assert merged.counters.true_count == 4
    assert merged.number_tracker.ints.count == 0
    assert merged.number_tracker.floats.count == 4
    assert merged.string_tracker.count == 2
