import os
import datetime
import numpy as np

from whylogs import get_or_create_session
from whylogs.viz import profile_viewer


def test_profile_viewer(tmpdir):
    _session=None
    session = get_or_create_session()

    with session.logger("mytestytest",dataset_timestamp=datetime.datetime(2021, 6, 2)) as logger:
        for _ in range(5):
            logger.log({"uniform_integers": np.random.randint(0,50)})
            logger.log({"nulls": None})

        profile=logger.profile
    profile_viewer(profiles=[profile],output_path=tmpdir+"my_test.html")
    assert os.path.exists(tmpdir+"my_test.html")