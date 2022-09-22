from datetime import datetime

from whylogs.api.store.query import DateQuery


def test_larger_start_goes_to_end():
    config = DateQuery(start_date=datetime(2022, 1, 1), end_date=datetime(2021, 1, 1))

    assert config.start_date == datetime(2021, 1, 1)
    assert config.end_date == datetime(2022, 1, 1)


def test_start_equals_end_if_only_start():
    config = DateQuery(start_date=datetime(2022, 1, 1), end_date=None)

    assert config.start_date == datetime(2022, 1, 1)
    assert config.end_date == config.start_date
