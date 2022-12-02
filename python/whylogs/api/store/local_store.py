import logging
import os
import uuid
from datetime import datetime, timezone
from functools import reduce
from glob import glob
from typing import List, Optional, Union

import whylogs as why
from whylogs.api.store.profile_store import ProfileStore
from whylogs.api.store.query import DatasetIdQuery, DateQuery
from whylogs.api.writer.local import LocalWriter
from whylogs.core import DatasetProfileView

DEFAULT_DIR = "profile_store/"
logger = logging.getLogger(__name__)


class LocalStore(ProfileStore):
    """
    The LocalStore is the implementation of the base :class:ProfileStore
    that will manage reading and writing profiles on the local file system.

    In order to instantiate the object, you will need to define a dataset_id,
    which is related to the name of the model or dataset you're profiling.
    To properly use the LocalStore to generate files, you should append it
    to your existing Rolling Logger, as the below example demonstrates.

    ```python
    import whylogs as why
    from whylogs.api.store.local import LocalStore

    logger = why.logger(mode="rolling", interval=10, when="S", base_name="my_model")
    logger.append_store(store=LocalStore())

    new_df = model.predict(input_data)
    logger.log(new_df)
    ```

    The above piece of code will make sure to write the logged profiles
    to the correct location that can be further fetched using the same
    LocalStore, like so:

    ```python
    from datetime import datetime, timedelta

    from whylogs.api.store.date_config import DateConfig

    store = LocalStore()
    query = DateQuery(
        dataset_id="my_model",
        start_date = datetime.now(timezone.utc) - timedelta(days=7),
        end_date = datetime.now(timezone.utc)
    )

    profile_view = store.get(query=query)
    ```

    This will fetch all existing profiles from `my_model` from the past
    7 days in a single merged DatasetProfileView.

    To list existing profiles on your local ProfileStore, you can do:

    ```python
    store = LocalStore()
    store.list()
    ```

    >**NOTE**: The parameter `dataset_id` should always use the snake_case pattern,
    and it must also be **unique** to your existing dataset/ML model.
    If you use the same `dataset_id` to store different profiles,
    you will end up mixing those profiles and not being able to fetch and get
    them properly again.
    """

    def __init__(self):
        self._default_path = os.path.join(os.getcwd(), DEFAULT_DIR)
        logger.debug(f"Default ProfileStore path set to {self._default_path}")
        self._writer = LocalWriter(base_dir=self._default_path)
        if not os.path.isdir(self._default_path):
            os.makedirs(os.path.join(self._default_path))

    @staticmethod
    def _get_profile_filename() -> str:
        now = datetime.now(timezone.utc)
        return f"profile_{now.date()}_{now.hour}:{now.minute}:{now.second}_{uuid.uuid4()}.bin"

    def list(self) -> List[str]:
        return [listed_file.name for listed_file in os.scandir(self._default_path) if listed_file.is_dir()]

    def get(self, query: Union[DateQuery, DatasetIdQuery]) -> Optional[DatasetProfileView]:
        logger.debug("Fetching profiles with specified StoreQuery...")
        files_list = []
        base_directory = os.path.join(self._default_path, query.dataset_id)
        if isinstance(query, DateQuery):
            dates = self._get_dates(query=query)
            for date in dates:
                files_list.extend(glob(f"{base_directory}/profile_{date.strftime('%Y-%m-%d')}*.bin"))
        elif isinstance(query, DatasetIdQuery):
            files_list.extend(glob(f"{base_directory}/profile_*bin"))
        else:
            logger.warning("You must define a proper Query object")
            return None

        logger.debug(f"Profiles found! Number of profiles is {len(files_list)}")
        profiles_list = (why.read(file).view() for file in files_list)
        merged_profile = reduce(lambda x, y: x.merge(y), profiles_list)
        return merged_profile

    def write(self, profile_view: DatasetProfileView, dataset_id: str) -> None:
        if not os.path.isdir(os.path.join(self._default_path, dataset_id)):
            logger.debug(f"Creating directory for {dataset_id}")
            os.makedirs(os.path.join(self._default_path, dataset_id))

        profile_dest = os.path.join(self._default_path, dataset_id, self._get_profile_filename())
        logger.debug("Writing to profile to default store destination...")
        self._writer.write(file=profile_view, dest=profile_dest)
