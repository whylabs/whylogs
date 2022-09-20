import os
import shutil
from datetime import datetime, timedelta
from glob import glob
from pathlib import Path

import pytest

from whylogs.api.store.date_config import DateConfig
from whylogs.api.store.local import LocalStore
from whylogs.core import DatasetProfile, DatasetProfileView

DEFAULT_PATH = os.path.join(os.getcwd(), "profile_store")


class TestLocalStore(object):
    @pytest.fixture
    def store(self):
        store = LocalStore(base_name="test_name")
        return store

    @pytest.fixture
    def profile_view(self):
        profile_view = DatasetProfile().view()
        return profile_view

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(DEFAULT_PATH)

    def test_get_profile_timestamp(self):
        actual_timestamp = LocalStore._get_profile_timestamp()

        now = datetime.utcnow()

        assert isinstance(actual_timestamp, str)
        assert f"profile_{now.date()}_{now.hour}:{now.minute}" in actual_timestamp

    def test_init_gets_or_creates_directory(self):
        assert not os.path.isdir(os.path.join(DEFAULT_PATH, "other_name"))

        LocalStore(base_name="other_name")

        assert os.path.isdir(os.path.join(DEFAULT_PATH, "other_name"))

        LocalStore(base_name="other_name")

        assert os.path.isdir(os.path.join(DEFAULT_PATH, "other_name"))

    def test_write(self, store, profile_view):
        store.write(profile=profile_view)

        base_dir = os.path.join(DEFAULT_PATH, "test_name")
        profile_list = glob(f"{base_dir}/profile_*.bin")

        assert os.path.isfile(profile_list[0])
        assert len(profile_list) == 1

    def test_write_doenst_overwrite_same_timestamp(self, store, profile_view):
        store.write(profile=profile_view)
        store.write(profile=profile_view)

        base_dir = os.path.join(DEFAULT_PATH, "test_name")
        profile_list = glob(f"{base_dir}/profile_*.bin")

        assert os.path.isfile(profile_list[0])
        assert os.path.isfile(profile_list[1])
        assert len(profile_list) >= 2

    def test_get(self, store, profile_view):
        store.write(profile=profile_view)
        store.write(profile=profile_view)

        date_config = DateConfig(start_date=datetime.utcnow())
        read_profile = store.get(date_config=date_config)

        assert read_profile is not None
        assert isinstance(read_profile, DatasetProfileView)

        larger_date_config = DateConfig(
            start_date=datetime.utcnow() - timedelta(days=7),
            end_date=datetime.utcnow(),
        )
        read_profile = store.get(date_config=larger_date_config)

        assert read_profile is not None
        assert isinstance(read_profile, DatasetProfileView)

    def test_get_ignores_files_that_dont_match_pattern(self, store, profile_view):
        store.write(profile=profile_view)
        Path(os.path.join(store._default_path, store.base_name, "profile_2022-02-01_23123.bin")).touch()

        date_config = DateConfig(start_date=datetime.utcnow())
        assert store.get(date_config=date_config)
