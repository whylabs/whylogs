import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List

import pytest

from whylogs.api.store.query import DateQuery, ProfileNameQuery
from whylogs.api.store.sqlite_store import SQLiteStore
from whylogs.core import DatasetProfile, DatasetProfileView


class TestSQLiteStore:
    @classmethod
    def setup_class(cls):
        os.environ["SQLITE_STORE_LOCATION"] = ":memory:"
        os.environ["MERGE_PROFILE_PERIOD_HOURS"] = "1"

    @classmethod
    def teardown_class(cls):
        del os.environ["MERGE_PROFILE_PERIOD_HOURS"]
        del os.environ["SQLITE_STORE_LOCATION"]

    @pytest.fixture
    def store(self, profile_view):
        store = SQLiteStore()
        store.cur.execute("CREATE TABLE IF NOT EXISTS profile_store(id string, date datetime, profile blob);")
        query = "INSERT INTO profile_store (id, date, profile) VALUES (?, ?, ?);"
        values_tuple = ("my_profile", profile_view.creation_timestamp, profile_view.serialize())
        store.cur.execute(query, values_tuple)
        yield store
        del store

    def test_sqlite_store_with_closes_connection(self):
        test_query = "SELECT * FROM sqlite_schema"
        with SQLiteStore() as store:
            store.cur.execute(test_query)
        with pytest.raises(sqlite3.ProgrammingError):
            store.cur.execute(test_query)

    def test_sqlite_list(self, store):
        assert store.list() == ["my_profile"]

    def test_sqlite_get_by_name(self, store):
        name_query = ProfileNameQuery(profile_name="my_profile")
        profile = store.get(query=name_query)
        assert isinstance(profile, DatasetProfileView)

    def test_sqlite_get_by_date_range(self, store):
        now = datetime.now(timezone.utc)
        one_week_ago = now - timedelta(days=7)
        date_query = DateQuery(profile_name="my_profile", start_date=one_week_ago, end_date=now)
        profile = store.get(query=date_query)
        assert isinstance(profile, DatasetProfileView)

    def test_sqlite_get_by_start_date_only(self, store):
        now = datetime.now(timezone.utc)
        date_query = DateQuery(profile_name="my_profile", start_date=now)
        profile = store.get(query=date_query)
        assert isinstance(profile, DatasetProfileView)

    def test_gets_an_empty_profile_if_does_not_exist(self, store):
        name_query = ProfileNameQuery(profile_name="doesnt_exist")
        expected_profile = store.get(query=name_query)
        assert isinstance(expected_profile, DatasetProfileView)
        assert expected_profile.get_columns() == {}

    def test_write_to_sqlite(self, store, profile_view):
        store.write(profile_view=profile_view, profile_name="test_profile")

        sql_query = "SELECT * FROM profile_store WHERE id = 'test_profile'"
        actual = store.cur.execute(sql_query).fetchall()

        assert isinstance(actual, List)
        assert actual[0][0] == "test_profile"
        assert DatasetProfileView.deserialize(actual[0][2])

    def test_check_if_profile_exists(self, store, profile_view):
        profile_name = "test_profile"
        empty = store._check_if_profile_exists(profile_view=profile_view, profile_name=profile_name)

        assert empty.get_columns() == {}

        query = "INSERT INTO profile_store (id, date, profile) VALUES (?, ?, ?);"
        values_tuple = (
            profile_name,
            profile_view.creation_timestamp.strftime("%Y-%m-%d %H:%M:%s"),
            profile_view.serialize(),
        )
        store.cur.execute(query, values_tuple)
        store.conn.commit()

        res = store.cur.execute("SELECT * FROM profile_store WHERE id = ?", (profile_name,))
        assert len(res.fetchall()) > 0

        not_empty = store._check_if_profile_exists(profile_view=profile_view, profile_name=profile_name)

        assert not not_empty.get_columns() == {}

    def test_merge_if_profile_already_exists(self, store, profile_view):
        store.write(profile_view=profile_view, profile_name="merged_profile")
        store.write(profile_view=profile_view, profile_name="merged_profile")

        sql_query = "SELECT * FROM profile_store WHERE id = 'merged_profile'"
        result = store.cur.execute(sql_query).fetchall()

        assert len(result) == 1

        actual_profile = DatasetProfileView.deserialize(result[0][2])
        expected_profile = profile_view.merge(profile_view)

        assert actual_profile.get_columns().keys() == expected_profile.get_columns().keys()
        actual_counts = actual_profile.get_column("legs").get_metric("counts").n.value
        expected_counts = expected_profile.get_column("legs").get_metric("counts").n.value

        assert actual_counts == expected_counts

    def test_larger_timestamps_wont_merge_profiles(self, store):
        ts1 = datetime.utcnow()
        ts2 = datetime.utcnow() + timedelta(hours=10 * int(os.environ["MERGE_PROFILE_PERIOD_HOURS"]))

        view1 = DatasetProfile(creation_timestamp=ts1).view()
        view2 = DatasetProfile(creation_timestamp=ts2).view()

        store.write(profile_view=view1, profile_name="test_not_merge")
        store.write(profile_view=view2, profile_name="test_not_merge")

        sql_query = "SELECT * FROM profile_store WHERE id = 'test_not_merge'"
        result = store.cur.execute(sql_query).fetchall()
        assert len(result) == 2
