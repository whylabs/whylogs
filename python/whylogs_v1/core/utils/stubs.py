import logging
from typing import List, Type

logger = logging.getLogger("whylogs.core.utils.stubs")

try:
    import pandas as pd
except:  # noqa
    pass

try:
    import numpy as np
except:  # noqa
    if pd is not None:
        logger.error(
            "Pandas is installed but numpy is not. Your environment is probably broken."
        )


class NumpyStub(object):
    def __init__(self) -> None:
        logger.warning(
            "Numpy not detected. Install numpy to take advantage of ndarray."
        )

    @property
    def ndarray(self) -> Type[List]:
        pass


class PandasStub(object):
    @property
    def Series(self) -> Type[List]:
        pass

    @property
    def DataFrame(self) -> Type[List]:
        pass


if np is None:
    np = NumpyStub()

if pd is None:
    pd = PandasStub()

numpy = np
pandas = pd
