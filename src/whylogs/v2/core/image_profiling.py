import logging
from typing import Callable, Dict, List, Union

from whylogs.features.transforms import Brightness, Hue, Saturation

logger = logging.getLogger(__name__)

try:
    from PIL.Image import Image as ImageType
    from PIL.TiffImagePlugin import IFDRational
    from PIL.TiffTags import TAGS
except ImportError as e:
    ImageType = None
    logger.debug(str(e))
    logger.debug("Unable to load PIL; install Pillow for image support")

DEFAULT_IMAGE_FEATURES = [Hue(), Saturation(), Brightness()]

_METADATA_DEFAULT_ATTRIBUTES = [
    "ImageWidth",
    "ImageLength",
    "BitsPerSample",
    "Compression",
    "Quality",
    "PhotometricInterpretation" "SamplesPerPixel",
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


def image_loader(path: str = None) -> ImageType:
    from PIL import Image

    with open(path, "rb") as file_p:
        img = Image.open(file_p).copy()
        return img


class TrackImage:
    """
    This is a class that computes image features and visits profiles and so image features can be sketched.

    Attributes:
        feature_name (str): name given to this image feature, will prefix all image based features
        feature_transforms (List[Callable]): Feature transforms to be apply to image data.
        img (PIL.Image): the PIL.Image
        metadata_attributes (TYPE): metadata attributes to track
    """

    def __init__(
        self,
        filepath: str = None,
        img: ImageType = None,
        feature_transforms: List[Callable] = DEFAULT_IMAGE_FEATURES,
        feature_name: str = "",
        metadata_attributes: Union[str, List[str]] = _METADATA_DEFAULT_ATTRIBUTES,
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
        """
        Call method to add image data and metadata to associated profiles
        Args:
            profiles (Union[List[DatasetProfile],DatasetProfile]): DatasetProfile
        """
        if not isinstance(profiles, list):
            profiles = [profiles]

        if self.metadata_attributes is not None:
            metadata = get_pil_image_metadata(self.img)

        for each_transform in self.feature_transforms:
            transform_name = "{0}".format(each_transform)
            transformed_image = each_transform(self.img)
            for each_profile in profiles:

                each_profile.track_array(columns=[self.feature_name + transform_name], x=transformed_image)

                if self.metadata_attributes == "all":
                    each_profile.track(metadata)

                else:
                    for each_attr in self.metadata_attributes:
                        attribute_value = metadata.get(each_attr, None)
                        each_profile.track(self.feature_name + each_attr, attribute_value)


def get_pil_image_metadata(img: ImageType) -> Dict:
    """
    Grab metra data from a PIL Image

    Args:
        img (ImageType): PIL Image

    Returns:
        Dict: of metadata
    """
    metadata = {TAGS[k]: "{}".format(v) if (isinstance(v, IFDRational)) else v for k, v in dict(img.getexif()).items()}

    metadata.update({"ImageFormat": img.format})

    return metadata


def image_based_metadata(img):
    return {
        "ImagePixelWidth": img.width,
        "ImagePixelHeight": img.height,
        "Colorspace": img.mode,
        "ImageFormat": img.format,
    }
