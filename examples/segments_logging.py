import time

import pandas as pd

from whylogs import get_or_create_session

if __name__ == "__main__":
    df = pd.read_csv("data/lending_club_1000.csv")
    print(df.head())
    session = get_or_create_session()

    # example with 4 seperate loggers

    # logger with two specific segments
    with session.logger(
        "segment",
        segments=[
            [{"key": "home_ownership", "value": "RENT"}],
            [{"key": "home_ownership", "value": "MORTGAGE"}],
        ],
        cache_size=1,
    ) as logger:
        print(session.get_config())
        logger.log_dataframe(df)
        profile_seg = logger.segemented_profiles

    # logger with rotation with time and single key segment
    with session.logger(
        "rotated_segments",
        segments=["home_ownership"],
        with_rotation_time="s",
        cache_size=1,
    ) as logger:
        print(session.get_config())
        logger.log_dataframe(df)
        time.sleep(2)
        logger.log_dataframe(df)
        profile_seg = logger.segemented_profiles

    # logger with rotation with time and two keys segment
    with session.logger(
        "rotated_seg_two_keys",
        segments=["home_ownership", "sub_grade"],
        with_rotation_time="s",
        cache_size=1,
    ) as logger:
        print(session.get_config())
        logger.log_csv("data/lending_club_1000.csv")
        time.sleep(2)
        logger.log_dataframe(df)
        profile_seg = logger.segemented_profiles

    # logger
    with session.logger(
        "rotated_seg_two_keys",
        segments=["home_ownership"],
        profile_full_dataset=True,
        with_rotation_time="s",
        cache_size=1,
    ) as logger:
        print(session.get_config())
        logger.log_csv("data/lending_club_1000.csv")
        time.sleep(2)
        logger.log_dataframe(df)
        profile_seg = logger.segemented_profiles
        full_profile = logger.profile
        # each segment profile has a tag associated with the segment
        for k, prof in profile_seg.items():
            print(prof.tags)
