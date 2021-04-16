import sys
from typing import Dict, List

import pandas as pd
import pytest
from PIL.Image import Image as ImageType

from whylogs.io.file_loader import file_loader


def test_image_loader(image_files):
    imgfmts = ["TIFF", "JPEG", "BMP"]
    for idx, img_path in enumerate(image_files):
        (img, magic_data), imgfmt = file_loader(img_path)
        assert isinstance(img, ImageType)
        assert isinstance(imgfmt, str)
        assert imgfmt == imgfmts[idx]


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.7 or higher")
def test_file_loader(file_list):
    filefmts = [(pd.DataFrame, "csv"), (list, "jsonl")]

    # Our version of pandas doesn't support excel. If we upgrade to
    # pandas >= 1.2 then we can support excel
    # filefmts.append((pd.DataFrame, "excel"))
    supported_files = list(filter(lambda file: "xlsx" not in file, file_list))

    for idx, file_path in enumerate(supported_files):
        (data, magic_data), filefmt = file_loader(file_path)
        assert isinstance(data, filefmts[idx][0])
        assert isinstance(filefmt, str)
        assert filefmt == filefmts[idx][1]
