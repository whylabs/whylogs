import logging
import os
from datetime import datetime
from functools import reduce
from glob import glob
from typing import Optional

import whylogs as why
from whylogs.api.store.profile_store import ProfileStore
from whylogs.api.store.query import DateQuery
from whylogs.api.store.utils.random_strings import random_string
from whylogs.api.writer.local import LocalWriter
from whylogs.core import DatasetProfileView

DEFAULT_DIR = "profile_store/"
logger = logging.getLogger(__name__)

# TODO how to prevent users from creating a huge list?
# should the profile store be aware of its min/max dates?


class LocalStore(ProfileStore):
    """
    The LocalStore is the implementation of the base :class:ProfileStore
    that will manage reading and writing profiles on the local file system.

    In order to instantiate the object, you will need to define a base_name,
    which is related to the name of the model or dataset you're profiling.
    To properly use the LocalStore to generate files, you should append it
    to your existing Rolling Logger, as the below example demonstrates.

    ```python
    import whylogs as why
    from whylogs.api.store.local import LocalStore

    logger = why.logger(mode="rolling", interval=10, when="S")
    logger.append_store(store=LocalStore(base_name="my_model"))

    new_df = model.predict(input_data)
    logger.log(new_df)
    ```

    The above piece of code will make sure to write the logged profiles
    to the correct location that can be further fetched using the same
    LocalStore, like so:

    ```python
    from datetime import datetime, timedelta

    from whylogs.api.store.date_config import DateConfig

    store = LocalStore(base_name="my_model")
    date_config = DateConfig(
        start_date = datetime.utcnow() - timedelta(days=7),
        end_date = datetime.utcnow()
    )

    profile_view = store.get(date_config=date_config)
    ```

    This will fetch all existing profiles from `my_model` from the past
    7 days in a single merged DatasetProfileView.

    :param base_name: the unique name of the dataset or model that you're logging
    :type base_name: str

    >**NOTE**: The parameter `base_name` should always be written using
    the snake_case pattern, and it must also be **unique** to your existing
    dataset/ML model. If you use the same `base_name` to store different profiles,
    you will end up mixing those profiles and not being able to fetch and get
    them properly again.
    """

    def __init__(self, base_name: str):
        self.base_name = base_name
        self._default_path = os.path.join(os.getcwd(), DEFAULT_DIR)
        logger.debug(f"Default ProfileStore path set to {self._default_path}")
        self._writer = LocalWriter(base_dir=self._default_path)

    @staticmethod
    def _get_profile_filename() -> str:
        now = datetime.utcnow()
        return f"profile_{now.date()}_{now.hour}:{now.minute}:{now.second}_{random_string()}.bin"

    def get(self, query: DateQuery) -> Optional[DatasetProfileView]:
        logger.debug("Fetching profiles with specified StoreQuery...")
        dates = self._get_dates(query=query)
        files_list = []
        base_directory = os.path.join(self._default_path, self.base_name)

        if isinstance(query, DateQuery):
            for date in dates:
                files_list.extend(glob(f"{base_directory}/profile_{date}*.bin"))
        else:
            logger.warning("You must define a proper StoreQuery")
            return None

        logger.debug(f"Profiles found! Number of profiles is {len(files_list)}")

        profiles_list = (why.read(file).view() for file in files_list)
        merged_profile = reduce(lambda x, y: x.merge(y), profiles_list)
        return merged_profile

    def write(self, profile: DatasetProfileView) -> None:
        if not os.path.isdir(os.path.join(self._default_path, self.base_name)):
            logger.debug(f"Creating directory for {self.base_name}")
            os.makedirs(os.path.join(self._default_path, self.base_name))

        profile_dest = os.path.join(self._default_path, self.base_name, self._get_profile_filename())
        logger.debug("Writing to profile to default store destination...")
        self._writer.write(file=profile, dest=profile_dest)
