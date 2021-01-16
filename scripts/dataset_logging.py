import pandas as pd
import time
from PIL import Image
from PIL.ExifTags import TAGS
from whylogs.core.datasetprofile import dataframe_profile

from whylogs.io.local_dataset import LocalDataset
from whylogs import get_or_create_session

# from torch.util import Dataset
import whylogs.core.image_file as logIM

if __name__ == "__main__":
    df = pd.read_csv("data/lending-club-accepted-10.csv")
    print(df.head())
    session = get_or_create_session()
    single_img_path= "data/sample_images/store1_known/5d46f2f5141f282b87874159cbfa6020_17_Eh_10.jpg"
    dset= LocalDataset(root_folder="data/sample_images/")
    print(dset[0])
    print(len(dset))
    with session.logger(
        "dataset_1", cache=1
    ) as logger:
        # print(session.get_config())
        profile= logger.profile
        trackimage=logIM.TrackImage(single_img_path,"Image_")
        trackimage(profile)
        # logIM.log_image_file(single_img_path,[profile],prefix="Image_")
        # summary = profile.to_summary()
        # # print(summary)
        print(profile.columns)
        # profile = logger.log_dataset(path="data/sample_images/")


        # log(feature_name= , data=, extracted_feature= )
    
#  