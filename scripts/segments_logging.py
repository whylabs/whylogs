import pandas as pd
import time
from whylogs.core.datasetprofile import dataframe_profile
from whylogs import get_or_create_session

if __name__ == "__main__":
    df = pd.read_csv("data/lending-club-accepted-10.csv")
    print(df.head())
    session = get_or_create_session()

    with session.logger(
        "segment", segments=[[{"key": "home_ownership", "value": "RENT"}], [{"key": "home_ownership", "value": "MORTGAGE"}]], cache=1
    ) as logger:
        print(session.get_config())
        logger.log_dataframe(df)
        profile_seg = logger.segemented_profiles

    with session.logger("my_rotated_seg", segments=["home_ownership"], with_rotation_time="s", cache=1) as logger:
        print(session.get_config())
        logger.log_dataframe(df)
        time.sleep(2)
        logger.log_dataframe(df)
        profile_seg = logger.segemented_profiles

    with session.logger("my_rotated_seg_two_keys", segments=["home_ownership", "sub_grade"], with_rotation_time="s", cache=1) as logger:
        print(session.get_config())
        logger.log_csv("data/lending-club-accepted-10.csv")
        time.sleep(2)
        logger.log_dataframe(df)
        profile_seg = logger.segemented_profiles

    with session.logger("my_rotated_seg_two_keys", segments=["home_ownership"], profile_full_dataset=True, with_rotation_time="s", cache=1) as logger:
        print(session.get_config())
        logger.log_csv("data/lending-club-accepted-10.csv")
        time.sleep(2)
        logger.log_dataframe(df)
        profile_seg = logger.segemented_profiles
        full_profile = logger.profile
