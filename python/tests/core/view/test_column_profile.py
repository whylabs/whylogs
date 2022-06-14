import logging
import pickle
import unittest
from typing import Any, Dict

import numpy as np
import pandas as pd

from whylogs.core import ColumnProfile, ColumnProfileView, ColumnSchema
from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics.metrics import (
    DistributionMetric,
    FrequentItem,
    FrequentItemsMetric,
)

TEST_LOGGER = logging.getLogger(__name__)


def check_dict_equality_and_compare_nan(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False

    keys_a = a.keys()
    keys_b = b.keys()

    if len(keys_a) != len(keys_b):
        return False

    for key in keys_a:
        if isinstance(a[key], list):
            if a[key] == b[key]:
                continue
            else:
                return False
        elif pd.isna(a[key]) and pd.isna(b[key]):
            continue

        if a[key] != b[key]:
            return False
    return True


class TestColumnProfile(unittest.TestCase):
    def test_basic_int_column(self) -> None:
        series = pd.Series(list(range(0, 10)))
        schema = ColumnSchema(dtype=series.dtype)
        col_prof = ColumnProfile("numbers", schema, cache_size=1024)
        col_prof.track_column(series)
        col_prof.track_column(np.array(range(0, 100), float))
        col_prof.track_column(np.array(range(0, 100), int))
        col_prof.track_column(["a", "b", "c"])
        col_prof.flush()

        # must have dist and types
        assert col_prof._metrics["distribution"] is not None
        assert col_prof._metrics["types"] is not None

        msg = col_prof.to_protobuf()
        all_keys = set(msg.metric_components.keys())
        assert "frequent_items/frequent_strings" in all_keys
        assert "counts/n" in all_keys
        assert "counts/null" in all_keys
        assert "distribution/kll" in all_keys
        assert "distribution/mean" in all_keys
        assert "distribution/m2" in all_keys
        assert "types/integral" in all_keys
        assert "types/boolean" in all_keys
        assert "types/object" in all_keys
        assert "types/string" in all_keys
        assert "types/fractional" in all_keys
        assert "ints/min" in all_keys
        assert "ints/max" in all_keys
        assert "cardinality/hll" in all_keys

        view = ColumnProfileView.from_protobuf(msg)
        assert view.get_metric("distribution") is not None

    def test_basic_str_column(self) -> None:
        series = pd.Series(["a", "b", "c"])
        schema = ColumnSchema(series.dtype)
        col_prof = ColumnProfile("string", schema, cache_size=1024)
        col_prof.track_column(series)
        col_prof.flush()

        msg = col_prof.to_protobuf()
        view = ColumnProfileView.from_protobuf(msg)
        assert view.get_metric("distribution") is not None

        # histogram should be None
        distribution: DistributionMetric = col_prof._metrics.get("distribution")
        frequent_item: FrequentItemsMetric = col_prof._metrics.get("frequent_items")
        assert distribution.n == 0
        assert frequent_item is not None
        frequent_strings = frequent_item.to_summary_dict(cfg=SummaryConfig())["frequent_strings"]
        assert frequent_strings is not None
        assert FrequentItem(value="a", est=1, upper=1, lower=1) in frequent_strings

    def test_basic_serialization_roundtrip(self) -> None:
        series = pd.Series(["a", "b", "c"])
        schema = ColumnSchema(series.dtype)
        col_prof = ColumnProfile("string", schema, cache_size=1024)
        col_prof.track_column(series)
        col_prof.flush()

        view = col_prof.view()
        view_roundtrip = ColumnProfileView.deserialize(view.serialize())
        assert view_roundtrip.get_metric("frequent_items") is not None
        TEST_LOGGER.debug(view_roundtrip.to_summary_dict())
        if not check_dict_equality_and_compare_nan(view_roundtrip.to_summary_dict(), view.to_summary_dict()):
            assert view_roundtrip.to_summary_dict() == view.to_summary_dict()

    def test_basic_pickle_roundtrip(self) -> None:
        series = pd.Series(["a", "b", "c", "C", "c", 2, 3, 3, 3, 5, 5, 5, 5, 5, 5])
        schema = ColumnSchema(series.dtype)
        col_prof = ColumnProfile("string", schema, cache_size=1024)
        col_prof.track_column(series)
        col_prof.flush()

        view = col_prof.view()
        pickle_view_bytes = pickle.dumps(view)
        view_roundtrip = pickle.loads(pickle_view_bytes)
        assert view_roundtrip.get_metric("frequent_items") is not None
        TEST_LOGGER.debug(view_roundtrip.to_summary_dict())
        if not check_dict_equality_and_compare_nan(view_roundtrip.to_summary_dict(), view.to_summary_dict()):
            assert sorted(view_roundtrip.to_summary_dict()) == sorted(view.to_summary_dict())


if __name__ == "__main__":
    unittest.main()
