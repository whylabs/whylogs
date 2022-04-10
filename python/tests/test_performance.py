import cProfile
import pstats
from io import StringIO
from logging import getLogger

import numpy as np
import pandas as pd
import pytest

from whylogs_v1.core import ColumnProfile, ColumnSchema

TEST_LOGGER = getLogger(__name__)


@pytest.mark.load
def test_track_column_benchmark() -> None:
    row_counts = [1200000]
    TEST_LOGGER.info("Running log_dataframe_benchmark")
    for i in range(len(row_counts)):
        full_df = pd.DataFrame((np.random.rand(row_counts[i], 1) - 0.5) * 3, columns=["A"])

        # Create a whylogs-v1 profile
        string_output_stream = StringIO()
        profiler = cProfile.Profile()
        profiler.enable()
        col_prof = ColumnProfile(name="perf_test", schema=ColumnSchema(float), cache_size=1024)
        col_prof.track_column(full_df["A"])
        profiler.disable()
        stats = pstats.Stats(profiler, stream=string_output_stream).sort_stats("cumulative")
        stats.print_stats(40)
        TEST_LOGGER.info(f"stats for dataframe of shape: {full_df.shape} are \n{string_output_stream.getvalue()}")

        assert col_prof._trackers["hist"] is not None
        TEST_LOGGER.info(f"Benchmark was using the following trackers {col_prof._trackers.keys()}")
