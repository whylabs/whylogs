import time

import pandas as pd

from whylogs import get_or_create_session

if __name__ == "__main__":
    df = pd.read_csv("data/lending-club-accepted-10.csv")

    session = get_or_create_session()
    with session.logger("test", with_rotation_time="s", cache_size=1) as logger:
        logger.log_dataframe(df)
        time.sleep(2)
        logger.log_dataframe(df)
        logger.log_dataframe(df)
        time.sleep(2)
        logger.log_dataframe(df)
