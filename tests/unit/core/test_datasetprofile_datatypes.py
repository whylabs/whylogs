

from whylogs.core.datasetprofile import DatasetProfile


def test_track_null_item():
    prof = DatasetProfile("name")
    prof.track("column_name", 1)
    prof = DatasetProfile("name")
    prof.track("column_name", None)
    assert prof.flat_summary()["summary"]["column"][0] == "column_name"
    assert prof.flat_summary()["summary"]["null_count"][0] == 1
    prof.track("column_name", None)
    assert prof.flat_summary()["summary"]["null_count"][0] == 2
    assert prof.flat_summary()["summary"]["column"][0] == "column_name"
