import os
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from whylogs.api.store.query import DatasetIdQuery, DateQuery
from whylogs.api.store.sqlite_store import SQLiteStore
from whylogs.core import DatasetProfile, DatasetProfileView


class TestSQLiteStore:
    @classmethod
    def setup_class(cls):
        os.environ["SQLITE_STORE_LOCATION"] = "test_database.db"
        os.environ["MERGE_PROFILE_PERIOD_HOURS"] = "1"

    @classmethod
    def teardown_class(cls):
        del os.environ["MERGE_PROFILE_PERIOD_HOURS"]
        del os.environ["SQLITE_STORE_LOCATION"]
        os.remove("test_database.db")

    @pytest.fixture
    def store(self, profile_view):
        with SQLiteStore() as store:
            store.write(profile_view=profile_view, dataset_id="my_profile")
            yield store

    def test_sqlite_list(self, store):
        assert store.list() == ["my_profile"]

    def test_sqlite_get_by_name(self, store):
        name_query = DatasetIdQuery(dataset_id="my_profile")
        profile = store.get(query=name_query)
        assert isinstance(profile, DatasetProfileView)

    def test_sqlite_get_by_date_range(self, store):
        now = datetime.now(timezone.utc)
        one_week_ago = now - timedelta(days=7)
        date_query = DateQuery(dataset_id="my_profile", start_date=one_week_ago, end_date=now)
        profile = store.get(query=date_query)
        assert isinstance(profile, DatasetProfileView)

    def test_sqlite_get_by_start_date_only(self, store):
        now = datetime.now(timezone.utc)
        date_query = DateQuery(dataset_id="my_profile", start_date=now)
        profile = store.get(query=date_query)
        assert isinstance(profile, DatasetProfileView)

    def test_gets_an_empty_profile_if_does_not_exist(self, store):
        name_query = DatasetIdQuery(dataset_id="doesnt_exist")
        expected_profile = store.get(query=name_query)
        assert isinstance(expected_profile, DatasetProfileView)
        assert expected_profile.get_columns() == {}

    def test_write_to_sqlite(self, store, profile_view):
        store.write(profile_view=profile_view, dataset_id="test_profile")

        assert len(store.list()) > 0
        assert "test_profile" in store.list()

    def test_check_if_profile_exists(self, store, profile_view):
        dataset_id = "test_profile_exists"
        empty = store._check_if_profile_exists(profile_view=profile_view, dataset_id=dataset_id)

        assert empty.get_columns() == {}

        store.write(profile_view=profile_view, dataset_id=dataset_id)

        not_empty = store._check_if_profile_exists(profile_view=profile_view, dataset_id=dataset_id)
        assert not_empty.get_columns() != {}

    def test_merge_if_profile_already_exists(self, store, profile_view):
        store.write(profile_view=profile_view, dataset_id="merged_profile")
        store.write(profile_view=profile_view, dataset_id="merged_profile")

        assert "merged_profile" in store.list()
        query = DatasetIdQuery(dataset_id="merged_profile")

        actual_profile = store.get(query=query)
        expected_profile = profile_view.merge(profile_view)

        assert actual_profile.get_columns().keys() == expected_profile.get_columns().keys()
        actual_counts = actual_profile.get_column("legs").get_metric("counts").n.value
        expected_counts = expected_profile.get_column("legs").get_metric("counts").n.value

        assert actual_counts == expected_counts

    def test_larger_timestamps_wont_merge_profiles(self):
        with SQLiteStore() as store:
            ts1 = datetime.utcnow()
            ts2 = datetime.utcnow() + timedelta(hours=10 * int(os.environ["MERGE_PROFILE_PERIOD_HOURS"]))

            view1 = DatasetProfile(creation_timestamp=ts1).view()
            view2 = DatasetProfile(creation_timestamp=ts2).view()

            store.write(profile_view=view1, dataset_id="test_not_merge")
            store.write(profile_view=view2, dataset_id="test_not_merge")

        con = sqlite3.connect(os.environ["SQLITE_STORE_LOCATION"])
        cur = con.cursor()
        sql_query = "SELECT * FROM profile_store WHERE id = 'test_not_merge'"
        result = cur.execute(sql_query).fetchall()
        assert len(result) == 2
