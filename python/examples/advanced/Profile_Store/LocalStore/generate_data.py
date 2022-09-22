import time

import pandas as pd

import whylogs as why
from whylogs.api.store.local_store import LocalStore

logger = why.logger(mode="rolling", interval=10, when="S")
logger.append_store(store=LocalStore(base_name="test_constraints"))


def predict():
    new_df = pd.DataFrame(data={"a": [1, 1, 2], "b": [11, 12, 2], "c": [2, 2, 2]})
    logger.log(new_df)


if __name__ == "__main__":
    for i in range(20):
        predict()
        time.sleep(1)
    logger.close()
