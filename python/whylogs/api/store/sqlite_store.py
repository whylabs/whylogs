import logging
import os
import sqlite3
from datetime import timedelta, timezone

from whylogs.api.store import ProfileStore
from whylogs.api.store.query import BaseQuery, DatasetIdQuery, DateQuery
from whylogs.core import DatasetProfile, DatasetProfileView

logger = logging.getLogger(__name__)


class SQLiteStore(ProfileStore):
    def __init__(self):
        self._db_location = os.getenv("SQLITE_STORE_LOCATION")
        if self._db_location is None:
            raise ValueError("You must define a store with the SQLITE_STORE_LOCATION env var")
        self.__conn = sqlite3.connect(database=self._db_location, check_same_thread=False)
        self.__cur = self.__conn.cursor()
        self._init_db()

    def __enter__(self) -> "SQLiteStore":
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> None:
        try:
            self.__cur.close()
            self.__conn.close()
        except exception_type:
            logger.error(traceback)
            raise exception_value

    def __del__(self) -> None:
        try:
            self.__cur.close()
            self.__conn.close()
        except Exception as e:
            logger.debug(f"Connection not established. Error: {e}")

    def _init_db(self):
        init_db_query = "CREATE TABLE IF NOT EXISTS profile_store(id string, date datetime, profile blob);"
        self.__cur.execute(init_db_query)

    def list(self):
        sql_query = "SELECT DISTINCT id FROM profile_store;"
        response = self.__cur.execute(sql_query)
        return [item[0] for item in response]

    def get(self, query: BaseQuery) -> DatasetProfileView:
        available_profiles = self.list()
        if query.dataset_id not in available_profiles:
            return DatasetProfile().view()
        if isinstance(query, DatasetIdQuery):
            sql_query = "SELECT profile FROM profile_store WHERE id = ?"
            data_tuple = (query.dataset_id,)
            response = self.__cur.execute(sql_query, data_tuple).fetchall()
        elif isinstance(query, DateQuery):
            start_date = query.start_date.replace(minute=0).astimezone(tz=timezone.utc)
            end_date = query.end_date.astimezone(tz=timezone.utc) + timedelta(minutes=1)
            sql_query = f"SELECT profile FROM profile_store WHERE id = '{query.dataset_id}' AND date BETWEEN '{start_date}' AND '{end_date}';"
            response = self.__cur.execute(sql_query).fetchall()
        else:
            logger.error("Define a supported Query object")
            raise ValueError

        profile_view = DatasetProfileView.zero()
        for item in response:
            profile_view = profile_view.merge(DatasetProfileView.deserialize(item[0]))
        return profile_view

    def _insert_blob(self, profile_view: DatasetProfileView, dataset_id: str):
        try:
            profile_date = profile_view.creation_timestamp
            serialized_profile = profile_view.serialize()
            query = "INSERT INTO profile_store (id, date, profile) VALUES (?, ?, ?);"
            values_tuple = (dataset_id, profile_date, serialized_profile)
            self.__cur.execute(query, values_tuple)
            self.__conn.commit()
            logger.debug(f"Profile {dataset_id} successfully written to the Store!")
        except sqlite3.Error as e:
            logger.error(f"Error: {e}")

    @staticmethod
    def _get_merge_period() -> int:
        merge_period = os.getenv("MERGE_PROFILE_PERIOD_HOURS")
        if not merge_period:
            raise ValueError("You must define a MERGE_PROFILE_PERIOD_HOURS env var")
        return int(merge_period)

    def _check_if_profile_exists(self, profile_view: DatasetProfileView, dataset_id: str) -> DatasetProfileView:
        merge_period = self._get_merge_period()

        start_date = profile_view.creation_timestamp - timedelta(hours=merge_period)
        end_date = profile_view.creation_timestamp + timedelta(minutes=1)
        date_query = DateQuery(dataset_id=dataset_id, start_date=start_date, end_date=end_date)

        existing_profile = self.get(query=date_query)
        return existing_profile

    def _update_profile(self, profile_view: DatasetProfileView, dataset_id: str) -> None:
        merge_period = self._get_merge_period()

        serialized_profile = profile_view.serialize()
        start_date = profile_view.creation_timestamp - timedelta(hours=merge_period)
        end_date = profile_view.creation_timestamp + timedelta(minutes=1)

        update_values = (serialized_profile, dataset_id, start_date, end_date)
        update_query = """
            UPDATE profile_store
            SET profile = ?
            WHERE id = ?
            AND date BETWEEN ? AND ?
        """
        try:
            self.__cur.execute(update_query, update_values)
        except sqlite3.Error as e:
            logger.error(f"Error: {e}")

    def write(self, profile_view: DatasetProfileView, dataset_id: str):
        existing_profile = self._check_if_profile_exists(profile_view=profile_view, dataset_id=dataset_id)
        if existing_profile.get_columns() == {}:
            self._insert_blob(profile_view=profile_view, dataset_id=dataset_id)
        else:
            merged_profile = existing_profile.merge(profile_view)
            self._update_profile(profile_view=merged_profile, dataset_id=dataset_id)
