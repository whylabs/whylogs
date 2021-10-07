import logging
from typing import Callable, Dict, List, Union

logger = logging.getLogger(__name__)

try:
    from PIL.Image import Image as ImageType
    from PIL.ImageStat import Stat
    from PIL.TiffImagePlugin import IFDRational
    from PIL.TiffTags import TAGS
except ImportError as e:
    ImageType = None
    logger.debug(str(e))
    logger.debug("Unable to load PIL; install Pillow for image support")

DEFAULT_IMAGE_FEATURES = []


_DEFAULT_TAGS_ATTRIBUTES = [
    "ImagePixelWidth",
    "ImagePixelHeight",
    "Colorspace",
]
_IMAGE_HSV_CHANNELS = ["Hue", "Saturation", "Brightness"]
_STATS_PROPERTIES = ["mean", "stddev"]
_DEFAULT_STAT_ATTRIBUTES = [c + "." + s for c in _IMAGE_HSV_CHANNELS for s in _STATS_PROPERTIES]
_METADATA_DEFAULT_ATTRIBUTES = _DEFAULT_STAT_ATTRIBUTES + _DEFAULT_TAGS_ATTRIBUTES


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

        for each_profile in profiles:
            # Turn off feature transforms + logging by default, they can be expensive if they don't reduce dimensionality.
            for each_transform in self.feature_transforms:
                transform_name = "{0}".format(each_transform)
                transformed_image = each_transform(self.img)
                each_profile.track_array(columns=[self.feature_name + transform_name], x=transformed_image)

            if self.metadata_attributes == "all":
                each_profile.track(metadata)

            else:
                for each_attr in self.metadata_attributes:
                    attribute_value = metadata.get(each_attr, None)
                    each_profile.track(self.feature_name + each_attr, attribute_value)


def get_pil_image_statistics(img: ImageType, channels: List[str] = _IMAGE_HSV_CHANNELS, image_stats: List[str] = _STATS_PROPERTIES) -> Dict:
    """
    Compute statistics data for a PIL Image

    Args:
        img (ImageType): PIL Image

    Returns:
        Dict: of metadata
    """

    stats = Stat(img.convert("HSV"))
    metadata = {}
    for index in range(len(channels)):
        for statistic_name in image_stats:
            if hasattr(stats, statistic_name):
                metadata[channels[index] + "." + statistic_name] = getattr(stats, statistic_name)[index]

    return metadata


def get_pil_image_metadata(img: ImageType) -> Dict:
    """
    Grab statistics data from a PIL ImageStats.Stat

    Args:
        img (ImageType): PIL Image

    Returns:
        Dict: of metadata
    """
    metadata = {}
    for k, v in dict(img.getexif()).items():
        try:
            if isinstance(v, IFDRational):
                metadata[TAGS[k]] = "{}".format(v)
            else:
                metadata[TAGS[k]] = v
        except KeyError:
            logger.warning(f"Couldn't read exif tag: {k} skipping.")

    metadata.update(image_based_metadata(img))
    metadata.update(get_pil_image_statistics(img))

    return metadata


def image_based_metadata(img):
    return {
        "ImagePixelWidth": img.width,
        "ImagePixelHeight": img.height,
        "Colorspace": img.mode,
    }
