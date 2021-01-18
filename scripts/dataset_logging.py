import pandas as pd
import time
from PIL import Image
from PIL.ExifTags import TAGS
from whylogs.core.datasetprofile import dataframe_profile

from whylogs.io.local_dataset import LocalDataset
from whylogs import get_or_create_session


if __name__ == "__main__":
    df = pd.read_csv("data/lending-club-accepted-10.csv")
    session = get_or_create_session()

    with session.logger(
        "dataset_1", cache=1
    ) as logger:

        profile = logger.profile
        logger.log_image("../testdata/images/flower2.jpg",
                         feature_name="Image_")

        summary = profile.to_summary()
        print(profile.columns)
