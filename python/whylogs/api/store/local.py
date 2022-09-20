import logging
import os
from datetime import datetime
from functools import reduce
from glob import glob
from typing import Optional

import whylogs as why
from whylogs.api.store.date_config import DateConfig
from whylogs.api.store.profile_store import ProfileStore
from whylogs.api.store.utils.random_strings import random_string
from whylogs.api.writer.local import LocalWriter
from whylogs.core import DatasetProfileView

DEFAULT_DIR = "profile_store/"
logger = logging.getLogger(__name__)


class LocalStore(ProfileStore):
    def __init__(self, base_name: str):
        self.base_name = base_name
        self._default_path = os.path.join(os.getcwd(), DEFAULT_DIR)
        logger.debug(f"Default ProfileStore path set to {self._default_path}")
        self._writer = LocalWriter(base_dir=self._default_path)

        if not os.path.isdir(os.path.join(self._default_path, self.base_name)):
            os.makedirs(os.path.join(self._default_path, self.base_name))

    @staticmethod
    def _get_profile_timestamp() -> str:
        now = datetime.utcnow()
        return f"profile_{now.date()}_{now.hour}:{now.minute}:{now.second}_{random_string()}.bin"

    def _read_profile(self, date_config: DateConfig) -> Optional[DatasetProfileView]:
        dates_list = self.get_dates_list(date_config=date_config)
        files_list = []
        base_directory = os.path.join(self._default_path, self.base_name)
        for date in dates_list:
            files_list.extend(glob(f"{base_directory}/profile_{date}*.bin"))
        if len(files_list) > 0:
            logger.debug(f"Profiles found! Number of profiles is {len(files_list)}")
            profiles_list = [why.read(file).view() for file in files_list]
            merged_profile = reduce(lambda x, y: x.merge(y), profiles_list)
            return merged_profile
        else:
            logger.debug(f"Could not find any profiles at {base_directory}")
            return None

    def get(self, date_config: DateConfig) -> DatasetProfileView:
        logger.debug("Fetching profiles with specified DateConfig...")
        profile = self._read_profile(date_config=date_config)
        return profile

    def write(self, profile: Optional[DatasetProfileView]) -> None:
        profile_dest = os.path.join(self._default_path, self.base_name, self._get_profile_timestamp())
        logger.debug("Writing to profile to default store destination")
        self._writer.write(file=profile, dest=profile_dest)
