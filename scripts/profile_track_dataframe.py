"""
"""
from whylabs.logs.core.datasetprofile import dataframe_profile
import pandas as pd
import os


NUM_RECORDS_AVAILABLE = [
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
]

if __name__ == "__main__":
    data_dir = "/Users/ibackus/data/lending-club"
    num_records = 10000

    # run
    fname = f"accepted_2007_to_2018Q4_{num_records}.parquet"
    fname = os.path.join(data_dir, fname)

    assert num_records in NUM_RECORDS_AVAILABLE
    assert os.path.isdir(data_dir)
    assert os.path.isfile(fname)

    df = pd.read_parquet(fname)
    profile = dataframe_profile(df, name="lending_club")
    # summary = profile.to_summary()
