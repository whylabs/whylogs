from PIL import Image

from PIL.ExifTags import TAGS as eTAGs

from PIL.TiffTags import TAGS
from PIL.TiffImagePlugin import IFDRational
from whylogs.core.datasetprofile import DatasetProfile

from typing import Optional, Callable, List, Union, Dict
import numpy as np

from whylogs.features import _IMAGE_FEATURES
from whylogs.features.transforms import Hue, Saturation, Brightness


DEFAULT_IMAGE_FEATURES = [Hue(), Saturation(), Brightness()]


_METADATA_DEFAULT_ATTRIBUTES = [
    "ImageWidth",
    "ImageLength",
    "BitsPerSample",
    "Compression",
    "Quality",
    "PhotometricInterpretation"
    "SamplesPerPixel",
    "Model",
    "Software",
    "ResolutionUnit",
    "X-Resolution",
    "Y-Resolution",
    "Orientation",
    "RowsPerStrip",
    "ExposureTime",
    "BrightnessValue",
    "Flash",
]


def image_loader(path: str = None)-> Image:

    with open(path, "rb") as file_p:
        img = Image.open(file_p).copy()
        return img


class TrackImage:

    def __init__(self,
                 filepath: str = None,
                 img: Image = None,
                 feature_transforms: List[Callable] = DEFAULT_IMAGE_FEATURES,
                 feature_name: str = "",
                 metadata_attributes: Optional[List[str]] = _METADATA_DEFAULT_ATTRIBUTES,
                 ):

        if filepath is None and img is None:
            raise ValueError("Need image filepath or image data")

        if filepath is not None:
            self.img = image_loader(filepath)
        else:
            self.img = img

        self.feature_transforms = feature_transforms

        if feature_transforms is None:
            self.feature_transforms = DEFAULT_IMAGE_FEATURES

        self.feature_name = feature_name
        self.metadata_attributes = metadata_attributes

    def __call__(self, profiles):

        if not isinstance(profiles, list):
            profiles = [profiles]

        if self.metadata_attributes is not None:
            metadata = get_pil_image_metadata(self.img)

        for each_transform in self.feature_transforms:
            transform_name = "{0}".format(each_transform)
            transformed_image = each_transform(self.img)
            for each_profile in profiles:

                each_profile.track_array(
                    columns=[self.feature_name+transform_name], x=transformed_image)

                if self.metadata_attributes == "all":
                    each_profile.track(metadata)

                else:
                    for each_attr in self.metadata_attributes:
                        attribute_value = metadata.get(each_attr, None)
                        each_profile.track(
                            self.feature_name+each_attr, attribute_value)


def get_pil_image_metadata(img: Image)->Dict:

    metadata = {TAGS[k]:  "{}".format(v) if (isinstance(v, IFDRational)) else v
                for k, v in dict(img.getexif()).items()
                }

    if metadata is None:
        metadata = image_based_metadata(img)
    else:
        metadata.update({"ImageFormat": img.format})

    return metadata


def image_based_metadata(img):

    return {"ImageWidth": img.width,
            "ImageHeight": img.height,
            "PhotometricInterpretation": img.mode,
            "ImageFormat": img.format
            }
