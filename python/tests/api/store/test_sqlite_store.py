import sqlite3
from typing import List
from datetime import datetime, timedelta

import pytest

from whylogs.api.store.query import DateQuery, ProfileNameQuery
from whylogs.api.store.sqlite_store import SQLiteStore
from whylogs.core import DatasetProfileView


@pytest.fixture
def connection():
    connection = sqlite3.connect(":memory:")
    return connection


@pytest.fixture
def store(profile_view, connection):
    store = SQLiteStore(connection=connection)
    store.write(profile_view=profile_view, profile_name="my_profile")
    store.write(profile_view=profile_view, profile_name="my_profile")
    yield store
    del store


def test_sqlite_store_with_closes_connection(connection):
    with SQLiteStore(connection=connection) as store:
        assert isinstance(store.list(), List)
    with pytest.raises(sqlite3.ProgrammingError):
        store.cur.execute("SELECT * FROM sqlite_schema")


def test_sqlite_list(store):
    assert store.list() == ["my_profile"]


def test_sqlite_get_by_name(store):
    name_query = ProfileNameQuery(profile_name="my_profile")
    profile = store.get(query=name_query)
    assert isinstance(profile, DatasetProfileView)


def test_sqlite_get_by_date_range(store):
    now = datetime.utcnow()
    one_week_ago = now - timedelta(days=7)
    date_query = DateQuery(profile_name="my_profile", start_date=one_week_ago, end_date=now)
    profile = store.get(query=date_query)
    assert isinstance(profile, DatasetProfileView)


def test_sqlite_get_by_start_date_only(store):
    now = datetime.utcnow()
    date_query = DateQuery(profile_name="my_profile", start_date=now)
    profile = store.get(query=date_query)
    assert isinstance(profile, DatasetProfileView)


def test_gets_an_empty_profile_if_does_not_exist(store, caplog):
    name_query = ProfileNameQuery(profile_name="doesnt_exist")
    with pytest.raises(ValueError):
        store.get(query=name_query)
    assert f"Profile {name_query.profile_name} does not exist!" in caplog.text
