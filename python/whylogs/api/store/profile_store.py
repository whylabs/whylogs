import os
from datetime import datetime
from functools import reduce
from glob import glob
from typing import Optional

import whylogs as why
from whylogs.api.store.utils.random_strings import random_string
from whylogs.api.store.utils.read_data import DateConfig, get_dates_list
from whylogs.api.writer.local import LocalWriter
from whylogs.core import DatasetProfileView

DEFAULT_DIR = "profile_store/"


class ProfileStore:
    def __init__(self, base_name: str):
        self.base_name = base_name
        self._default_path = os.path.join(os.getcwd(), DEFAULT_DIR)
        self._writer = LocalWriter(base_dir=self._default_path)

        if not os.path.isdir(os.path.join(self._default_path, self.base_name)):
            os.makedirs(os.path.join(self._default_path, self.base_name))

    @staticmethod
    def _get_profile_timestamp() -> str:
        now = datetime.utcnow()
        return f"profile_{now.date()}_{now.hour}:{now.minute}:{now.second}_{random_string()}.bin"

    def _read_profile(self, date_config: DateConfig) -> Optional[DatasetProfileView]:
        dates_list = get_dates_list(date_config=date_config)
        files_list = []
        base_directory = os.path.join(self._default_path, self.base_name)
        for date in dates_list:
            files_list.extend(glob(f"{base_directory}/profile_{date}*.bin"))
        if len(files_list) > 0:
            profiles_list = [why.read(file).view() for file in files_list]
            merged_profile = reduce(lambda x, y: x.merge(y), profiles_list)
            return merged_profile
        else:
            return None

    def get(self, date_config: DateConfig) -> DatasetProfileView:
        profile = self._read_profile(date_config=date_config)
        return profile

    def write(self, profile: Optional[DatasetProfileView]) -> None:
        profile_dest = os.path.join(self._default_path, self.base_name, self._get_profile_timestamp())
        self._writer.write(file=profile, dest=profile_dest)


if __name__ == "__main__":
    from whylogs.core import DatasetProfileView

    # profile_view = DatasetProfileView(dataset_timestamp=datetime.now(), creation_timestamp=datetime.now())
    # profile = DatasetProfile()
    store = ProfileStore(base_name="test_concurrency")
    # store.write(profile=profile)
    # store.write(profile=profile)
    config = DateConfig(
        base_name="test_concurrency",
        start_date=datetime(2022, 9, 1),
        end_date=datetime(2022, 9, 15),
    )
    print(store.get(date_config=config).to_pandas())
