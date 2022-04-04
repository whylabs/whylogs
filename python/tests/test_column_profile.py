import unittest

import pandas as pd

from whylogs_v1.core import ColumnProfile, ColumnSchema
from whylogs_v1.core.datatypes import NumericColumnType


class TestListElements(unittest.TestCase):
    def test_basic_int_column(self) -> None:
        series = pd.Series(list(range(0, 10)))

        col_prof = ColumnProfile(ColumnSchema("demo", NumericColumnType[int](int)))
        col_prof.track_column(series)

        # Need to validate something nested here
        assert col_prof._trackers["hist"] is not None


if __name__ == "__main__":
    unittest.main()
