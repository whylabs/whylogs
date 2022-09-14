import time

import pandas as pd

import whylogs as why
from whylogs.api.store.profile_store import ProfileStore

# create a 5-minutely rolling logger
logger = why.logger(mode="rolling", interval=1, when="S", base_name="test_base_name")
# # add a local writer
# logger.append_writer("local", base_dir="whylogs_output")

logger.append_store(store=ProfileStore(base_name="test_profile"))


def predict(df: pd.DataFrame):
    logger.log(df)
    new_df = pd.DataFrame([[1, 2, 3]], columns=["aa", "bb", "cc"])
    logger.log(new_df)


if __name__ == "__main__":
    for i in range(2):
        predict(pd.DataFrame([[i, 2]], columns=["a", "b"]))
        time.sleep(1)
    logger.close()
