import logging
from itertools import chain
from typing import Dict, List, Union

import whylogs as why
from whylogs.api.logger.result_set import ResultSet
from whylogs.core.datatypes import DataType
from whylogs.core.metrics.compound_metric import CompoundMetric
from whylogs.core.metrics.metrics import (
    DistributionMetric,
    FrequentItemsMetric,
    Metric,
    MetricConfig,
    OperationResult,
)
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.resolvers import Resolver
from whylogs.core.schema import ColumnSchema, DatasetSchema

logger = logging.getLogger(__name__)

try:
    from PIL.Image import Image as ImageType
    from PIL.Image import new as new_image
    from PIL.ImageStat import Stat
    from PIL.TiffImagePlugin import IFDRational
    from PIL.TiffTags import TAGS
except ImportError as e:
    ImageType = None
    logger.warning(str(e))
    logger.warning("Unable to load PIL; install Pillow for image support")

DEFAULT_IMAGE_FEATURES: List[str] = []


_DEFAULT_TAGS_ATTRIBUTES = [
    "ImagePixelWidth",
    "ImagePixelHeight",
    "Colorspace",
]
_IMAGE_HSV_CHANNELS = ["Hue", "Saturation", "Brightness"]
_STATS_PROPERTIES = ["mean", "stddev"]
_DEFAULT_STAT_ATTRIBUTES = [c + "." + s for c in _IMAGE_HSV_CHANNELS for s in _STATS_PROPERTIES]
_METADATA_DEFAULT_ATTRIBUTES = _DEFAULT_STAT_ATTRIBUTES + _DEFAULT_TAGS_ATTRIBUTES


def get_pil_image_statistics(
    img: ImageType, channels: List[str] = _IMAGE_HSV_CHANNELS, image_stats: List[str] = _STATS_PROPERTIES
) -> Dict:
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


class ImageMetric(CompoundMetric):
    def __init__(self, submetrics: Dict[str, Metric]):
        if ImageType is None:
            logger.error("Install Pillow for image support")
        super(ImageMetric, self).__init__(submetrics)

    @property
    def namespace(self) -> str:
        return "image"

    def columnar_update(self, view: PreprocessedColumn) -> OperationResult:
        submetric_names = self.submetrics.keys()
        count = 0
        for image in list(chain.from_iterable(view.raw_iterator())):
            if isinstance(image, ImageType):
                metadata = get_pil_image_metadata(image)
                for attr in metadata.keys():
                    if attr in submetric_names:
                        self.submetrics[attr].columnar_update(PreprocessedColumn.apply([metadata[attr]]))
                count += 1
        return OperationResult.ok(count)

    @classmethod
    def zero(cls, config: MetricConfig) -> "ImageMetric":
        dummy_image = new_image("HSV", (1, 1))
        metadata = get_pil_image_metadata(dummy_image)
        submetrics: Dict[str, Metric] = dict()
        for tag, value in metadata.items():
            if isinstance(value, int):
                submetrics[tag] = DistributionMetric.zero(config)
            elif isinstance(value, float):
                submetrics[tag] = DistributionMetric.zero(config)
            elif isinstance(value, str) and not config.fi_disabled:
                submetrics[tag] = FrequentItemsMetric.zero(config)

        return ImageMetric(submetrics)


"""
def log_image(
    profiles: Union[List[DatasetProfile],DatasetProfile],
    images: Union[Dict[str, ImageType], ImageType],
    default_column_name: str="image",
) -> None:
    profiles = profiles if isinstance(profiles, list) else [profiles]
    images = images if isinstance(images, dict) else {default_column_name: images}
    for profile in profiles:
        profile.track(row=images)
"""


def log_image(
    images: Union[ImageType, List[ImageType], Dict[str, ImageType]],
    default_column_prefix: str = "image",
) -> ResultSet:
    if isinstance(images, ImageType):
        images = {default_column_prefix: images}
    if isinstance(images, list):
        count = 1
        images = dict()
        for img in images:
            images[f"{default_column_prefix}_{count}"] = img
            count += 1

    if not isinstance(images, dict):
        raise ValueError("log_image must be passed an image, list of images, or dictionary of images")
    for img in images.values():
        if not isinstance(img, ImageType):
            raise ValueError("log_image must be passed an image, list of images, or dictionary of images")

    class ImageResolver(Resolver):
        def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
            return {ImageMetric.get_namespace(MetricConfig()): ImageMetric.zero(MetricConfig())}

    schema = DatasetSchema(types={key: ImageType for key in images.keys()}, resolvers=ImageResolver())
    return why.log(row=images, schema=schema)
