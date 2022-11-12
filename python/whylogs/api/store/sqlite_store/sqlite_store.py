import logging
import os
import sqlite3
from datetime import timedelta
from typing import Optional, Tuple, Union, Any

from whylogs.api.store import ProfileStore
from whylogs.api.store.query import DateQuery, ProfileNameQuery
from whylogs.core import DatasetProfile, DatasetProfileView

logger = logging.getLogger(__name__)


class SQLiteStore(ProfileStore):
    def __init__(self):
        self._db_location = os.getenv("SQLITE_STORE_LOCATION")
        if self._db_location is None:
            raise ValueError("You must define a store with the SQLITE_STORE_LOCATION env var")
        self.conn = sqlite3.connect(database=self._db_location)
        self.cur = self.conn.cursor()

    def __enter__(self) -> "SQLiteStore":
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> None:
        try:
            self.cur.close()
            self.conn.close()
        except exception_type:
            logger.error(traceback)
            raise exception_value

    def __del__(self) -> None:
        try:
            self.cur.close()
            self.conn.close()
        except Exception as e:
            logger.debug(f"Connection not established. Error: {e}")

    def list(self):
        sql_query = "SELECT DISTINCT id FROM profile_store;"
        response = self.cur.execute(sql_query)
        return [item[0] for item in response]

    def get(self, query: Union[ProfileNameQuery, DateQuery]):
        available_profiles = self.list()
        if query.profile_name not in available_profiles:
            logger.error(f"Profile {query.profile_name} does not exist!")
            raise ValueError
        if isinstance(query, ProfileNameQuery):
            sql_query = "SELECT profile FROM profile_store WHERE id = ?"
            data_tuple = (query.profile_name,)
            response = self.cur.execute(sql_query, data_tuple)
        elif isinstance(query, DateQuery):
            sql_query = "SELECT profile FROM profile_store WHERE id = ? AND date BETWEEN ? AND ?;"
            data_tuple = (query.profile_name, query.start_date, query.end_date) # type: ignore
            response = self.cur.execute(sql_query, data_tuple)
        else:
            logger.error("Define a supported Query: Union[ProfileNameQuery, DateQuery]")
            raise ValueError
        profile_view = DatasetProfile().view()
        for item in response.fetchall():
            profile_view.merge(DatasetProfileView.deserialize(item[0]))
        return profile_view

    def _insert_blob(self, profile_view: DatasetProfileView, profile_name: str):
        try:
            profile_date = profile_view.dataset_timestamp
            serialized_profile = profile_view.serialize()
            query = f"INSERT INTO profile_store (id, date, profile) VALUES (?, ?, ?);"
            values_tuple = (profile_name, profile_date, serialized_profile)
            self.cur.execute(query, values_tuple)
            self.conn.commit()
            logger.debug(f"Profile {profile_name} successfully written to the Store!")
        except sqlite3.Error as e:
            logger.error(f"Error: {e}")

    def write(self, profile_view: DatasetProfileView, profile_name: str):
        init_db_query = (
            "CREATE TABLE IF NOT EXISTS profile_store(id string, date datetime, profile blob);"
        )
        self.cur.execute(init_db_query)
        
        merge_period = os.getenv("MERGE_PROFILE_PERIOD_HOURS")
        if not merge_period:
            raise ValueError("You must define a MERGE_PROFILE_PERIOD_HOURS env var")
        start_date = profile_view.dataset_timestamp - timedelta(hours=int(merge_period))
        end_date = profile_view.dataset_timestamp
        
        query = DateQuery(profile_name=profile_name, start_date=start_date, end_date=end_date)
        existing_profile = self.get(query=query)
        
        existing_profile.merge(profile_view)
        
        self._insert_blob(profile_view=existing_profile, profile_name=profile_name)
