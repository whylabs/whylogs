import pandas as pd
from whylogs import get_or_create_session


if __name__ == "__main__":
    df = pd.read_csv("data/lending-club-accepted-10.csv")
    session = get_or_create_session()

    with session.logger("dataset_1", cache_size=1) as logger:

        profile = logger.profile
        logger.log_image("../testdata/images/flower2.jpg", feature_name="Image_")

        summary = profile.to_summary()
        print(profile.columns)
