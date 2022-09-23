import time
from datetime import datetime
from typing import Generator, List
from unittest.mock import MagicMock

from pytest import fixture

from whylogs.api.store.profile_store import ProfileStore
from whylogs.api.store.query import DateQuery


@fixture
def query():
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


def test_get_date_range_returns_generator(query):
    result = TestProfileClass()._get_date_range(query=query)
    assert isinstance(result, Generator)
    for date in result:
        assert date >= datetime(2022, 1, 1)


def test_get_dates_list(query):
    result = TestProfileClass()._get_dates(query=query)

    assert isinstance(result, List)
    for date in result:
        assert time.strptime(date, "%Y-%m-%d")


def test_get_dates_with_single_date(query):
    query.end_date = datetime(2022, 1, 1)
    result = TestProfileClass()._get_dates(query=query)
    assert isinstance(result, List)
    assert len(result) == 1
    assert time.strptime(result[0], "%Y-%m-%d")
