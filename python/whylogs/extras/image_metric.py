from dataclasses import dataclass
from io import BytesIO
from itertools import chain
from typing import Any, Dict, List, Tuple, Union

import logging
import numpy as np

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

from whylogs.core.configs import SummaryConfig
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.metrics.compound_metric import CompoundMetric
from whylogs.core.metrics.deserializers import deserializer
from whylogs.core.metrics.metric_components import (
    FractionalComponent,
    IntegralComponent,
    MetricComponent,
)
from whylogs.core.metrics.metrics import (
    DistributionMetric,
    FrequentItemsMetric,
    Metric,
    MetricConfig,
    OperationResult,
)
from whylogs.core.metrics.serializers import serializer
from whylogs.core.preprocessing import ListView, PreprocessedColumn
from whylogs.core.proto import MetricComponentMessage, MetricMessage



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



@dataclass
class ImageMetric(CompoundMetric):

    def __init__(self, submetrics: Dict[str, Metric]):
        super(ImageMetric, self).__init__(submetrics)

    @property
    def namespace(self) -> str:
        return "image"

    def columnar_update(self, view: PreprocessedColumn) -> OperationResult:
        submetric_names = self.submetrics.keys()
        for image in list(chain.from_iterable(view.raw_iterator())):
            if isinstance(image, ImageType):
                metadata = get_pil_image_metadata(image)
                for attr in metadata.keys():
                    if attr in submetric_names:
                        self.submetrics[attr].columnar_update(PreprocessedColumn.apply([metadata[attr]]))
        return OperationResult.ok(1)

    @classmethod
    def zero(cls, config: MetricConfig) -> "ImageMetric":
        return ImageMetric(submetrics={
            "ImageWidth": DistributionMetric.zero(config),
            "ImageLength": DistributionMetric.zero(config),
            "Hue.mean": DistributionMetric.zero(config),
            "Saturation.mean": DistributionMetric.zero(config),
            "Brightness.mean": DistributionMetric.zero(config),
        })


def log_image(
    profiles: Union[List[DatasetProfile],DatasetProfile],
    images: Union[Dict[str, ImageType], ImageType],
    default_column_name: str="image",
) -> None:
    profiles = profiles if isinstance(profiles, list) else [profiles]
    images = images if isinstance(images, dict) else {default_column_name: images}
    for profile in profiles:
        profile.track(row=images)
