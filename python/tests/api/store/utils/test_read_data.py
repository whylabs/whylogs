from datetime import datetime

import pytest

from whylogs.api.store.utils.read_data import DateConfig


@pytest.fixture
def date_config():
    config = DateConfig(
        start_date=datetime(2022, 9, 6),
        end_date=datetime(2022, 9, 8),
        base_name="test_base_name",
        base_dir="whylogs_output",
    )
    return config
