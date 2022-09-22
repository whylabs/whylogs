import time
from datetime import datetime
from typing import Generator, List
from unittest.mock import MagicMock

from pytest import fixture

from whylogs.api.store.profile_store import ProfileStore
from whylogs.api.store.query import DateQuery


@fixture
def date_config():
    config = MagicMock(
        wraps=DateQuery,
        start_date=datetime(2022, 1, 1),
        end_date=datetime(2022, 1, 2),
    )
    return config


class TestProfileClass(ProfileStore):
    def get(self):
        pass

    def write(self):
        pass


def test_get_date_range_returns_generator(date_config):
    result = TestProfileClass()._get_date_range(date_config=date_config)
    assert isinstance(result, Generator)
    for date in result:
        assert date >= datetime(2022, 1, 1)


def test_get_dates_list(date_config):
    result = TestProfileClass().get_dates_list(date_config=date_config)

    assert isinstance(result, List)
    for date in result:
        assert time.strptime(date, "%Y-%m-%d")


def test_get_dates_with_single_date(date_config):
    date_config.end_date = datetime(2022, 1, 1)
    result = TestProfileClass().get_dates_list(date_config=date_config)
    assert isinstance(result, List)
    assert len(result) == 1
    assert time.strptime(result[0], "%Y-%m-%d")
