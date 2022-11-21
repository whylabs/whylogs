import logging
import os
import sqlite3
from datetime import datetime
from typing import Union

from whylogs.api.store import ProfileStore
from whylogs.api.store.query import DateQuery, ProfileNameQuery
from whylogs.core import DatasetProfile, DatasetProfileView

logger = logging.getLogger(__name__)


class SQLiteStore(ProfileStore):
    def __init__(self):
        self._db_location = os.getenv("SQLITE_STORE_LOCATION") or "profile_store.db"
        self.conn = sqlite3.connect(database=self._db_location)
        self.cur = self.conn.cursor()

    def list(self):
        response = self.cur.execute("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
        return [item[0] for item in response.fetchall()]

    def get(self, query: Union[ProfileNameQuery, DateQuery]):
        available_profiles = self.list()
        if query.profile_name not in available_profiles:
            logger.error(f"Profile {query.profile_name} does not exist!")
            raise ValueError
        if isinstance(query, ProfileNameQuery):
            response = self.cur.execute(f"SELECT profile FROM {query.profile_name}")
            profile_view = DatasetProfile().view()
        elif isinstance(query, DateQuery):
            sql_query = f"SELECT profile FROM '{query.profile_name}' WHERE date BETWEEN '{query.start_date}' AND '{query.end_date}';"
            response = self.cur.execute(sql_query)
        profile_view = DatasetProfile().view()
        for item in response.fetchall():
            profile_view.merge(DatasetProfileView.deserialize(item[0]))
        return profile_view

    def _insert_blob(self, profile_view: bytes, profile_name: str):
        try:
            profile_date = datetime.utcnow()
            data_tuple = (profile_date, profile_view)
            query = f"INSERT INTO {profile_name} (date, profile) VALUES (?, ?);"

            self.cur.execute(query, data_tuple)
            self.conn.commit()
            logger.debug("Profile successfully written to DB!")
        except sqlite3.Error as e:
            logger.error(f"Error: {e}")

    def write(self, profile_view: DatasetProfileView, profile_name: str):
        init_db_query = (
            f"CREATE TABLE IF NOT EXISTS {profile_name}(id integer primary key, date datetime, profile blob);"
        )
        self.cur.execute(init_db_query)
        self._insert_blob(profile_name=profile_name, profile_view=profile_view.serialize())

    def __del__(self):
        try:
            self.cur.close()
            self.conn.close()
        except Exception as e:
            logger.debug(f"Connection not established. Error: {e}")
            return True
