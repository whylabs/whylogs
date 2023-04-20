import os

import pandas as pd

import whylogs as why
from whylogs.api.whylabs.log_reference import log_reference


def test_log_reference() -> None:
    os.environ["WHYLABS_HOST"] = "https://songbird.development.whylabsdev.com"
    df = pd.DataFrame({"a": [1, 2, 3, 4]})

    why.init(anonymous=True)

    result = log_reference(df)

    assert result.status_code == 200


# TODO guarantee it is possible to change alias + timestamp
