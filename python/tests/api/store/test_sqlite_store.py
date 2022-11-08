import os
from datetime import datetime, timedelta

import pytest

from whylogs.api.store.query import DateQuery, ProfileNameQuery
from whylogs.api.store.sqlite_store import SQLiteStore
from whylogs.core import DatasetProfileView


class TestSqliteStore:
    @classmethod
    def setup_class(cls):
        os.environ["SQLITE_STORE_LOCATION"] = "test_profile_store.db"

    @classmethod
    def teardown_class(cls):
        del os.environ["SQLITE_STORE_LOCATION"]
        os.remove("test_profile_store.db")

    @pytest.fixture
    def store(self, profile_view):
        store = SQLiteStore()
        store.write(profile_view=profile_view, profile_name="my_profile")
        store.write(profile_view=profile_view, profile_name="my_profile")
        return store

    def test_sqlite_list(self, store):
        assert store.list() == ["my_profile"]

    def test_sqlite_get_by_name(self, store):
        name_query = ProfileNameQuery(profile_name="my_profile")
        profile = store.get(query=name_query)
        assert isinstance(profile, DatasetProfileView)

    def test_sqlite_get_by_date_range(self, store):
        now = datetime.utcnow()
        one_week_ago = now - timedelta(days=7)
        date_query = DateQuery(profile_name="my_profile", start_date=one_week_ago, end_date=now)
        profile = store.get(query=date_query)
        assert isinstance(profile, DatasetProfileView)

    def test_sqlite_get_by_start_date_only(self, store):
        now = datetime.utcnow()
        date_query = DateQuery(profile_name="my_profile", start_date=now)
        profile = store.get(query=date_query)
        assert isinstance(profile, DatasetProfileView)

    def test_gets_an_empty_profile_if_does_not_exist(self, store, caplog):
        name_query = ProfileNameQuery(profile_name="doesnt_exist")
        with pytest.raises(ValueError):
            store.get(query=name_query)
        assert f"Profile {name_query.profile_name} does not exist!" in caplog.text
