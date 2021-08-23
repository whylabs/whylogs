import datetime
import os
import numpy as np


from whylogs.app.config import load_config
from whylogs.app.session import session_from_config
from whylogs.viz import profile_viewer

def test_profile_viewer(tmpdir,local_config_path):

    config = load_config(local_config_path)
    session = session_from_config(config)

    with session.logger("mytestytest", dataset_timestamp=datetime.datetime(2021, 6, 2)) as logger:
        for _ in range(5):
            logger.log({"uniform_integers": np.random.randint(0, 50)})
            logger.log({"nulls": None})

        profile = logger.profile
    result = profile_viewer(profiles=[profile], output_path=tmpdir + "my_test.html")
    assert os.path.exists(tmpdir + "my_test.html")
    assert result == tmpdir + "my_test.html"
