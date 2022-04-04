from typing import List, Union

from whylogs_v1.core.utils import numpy as np
from whylogs_v1.core.utils import pandas as pd

COMMON_COLUMNAR_TYPES = Union[pd.Series, np.ndarray, List]
