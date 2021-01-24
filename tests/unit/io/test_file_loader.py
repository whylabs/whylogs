
from whylogs.io.file_loader import file_loader
import os
from PIL.Image import Image as ImageType
import pandas as pd
from typing import Dict


def test_image_loader(image_files):
    imgfmts = ["TIFF", "JPEG", "BMP"]
    for idx, img_path in enumerate(image_files):
        img, imgfmt = file_loader(img_path)
        assert isinstance(img, ImageType)
        assert isinstance(imgfmt, str)
        assert imgfmt == imgfmts[idx]


def test_file_loader(file_list):

    filefmts = [(pd.DataFrame, "excel"),
                (pd.DataFrame, "csv"), (Dict,  "jsonl")]

    for idx, file_path in enumerate(file_list):
        data, filefmt = file_loader(file_path)
        assert isinstance(data, filefmts[idx][0])
        assert isinstance(filefmt, str)
        assert filefmt == filefmts[idx][1]
# def test_files_


# fsegment_A_target_files = [
#     os.path.join(folder_dataset, 'A_target', file) for file in ('flower2.jpg', 'grace_hopper_517x606.jpg',)
# ]
