from datetime import datetime

from whylogs.api.store.query import DateQuery


def test_larger_start_goes_to_end():
    query = DateQuery(start_date=datetime(2022, 1, 1), end_date=datetime(2021, 1, 1), profile_name="test")

    assert query.start_date == datetime(2021, 1, 1)
    assert query.end_date == datetime(2022, 1, 1)


def test_start_equals_end_if_only_start():
    query = DateQuery(start_date=datetime(2022, 1, 1), end_date=None, profile_name="test")

    assert query.start_date == datetime(2022, 1, 1)
    assert query.end_date == query.start_date
