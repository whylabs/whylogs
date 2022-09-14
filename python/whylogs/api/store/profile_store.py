import os
from datetime import datetime
from typing import Optional

from whylogs.api.store.utils.random_strings import random_string
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

    def _get_profile_timestamp(self) -> str:
        now = datetime.utcnow()
        return f"profile_{random_string()}_{now.date()}_{now.hour}:{now.minute}:{now.second}.bin"

    def get(self) -> DatasetProfileView:
        # TODO add get logic
        pass

    def write(self, profile: Optional[DatasetProfileView]) -> None:
        profile_dest = os.path.join(self._default_path, self.base_name, self._get_profile_timestamp())
        self._writer.write(file=profile, dest=profile_dest)


if __name__ == "__main__":
    from whylogs.core import DatasetProfile, DatasetProfileView

    # profile_view = DatasetProfileView(dataset_timestamp=datetime.now(), creation_timestamp=datetime.now())
    profile = DatasetProfile()
    store = ProfileStore(base_name="test_concurrency")
    store.write(profile=profile)
    store.write(profile=profile)
