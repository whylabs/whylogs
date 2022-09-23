import logging
from dataclasses import dataclass, field
from itertools import chain
from typing import Dict, List, Optional, Set, Union

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


@dataclass(frozen=True)
class ImageMetricConfig(MetricConfig):
    allowed_exif_tags: Set[str] = field(default_factory=set)
    forbidden_exif_tags: Set[str] = field(default_factory=set)


class ImageMetric(CompoundMetric):
    def __init__(
        self,
        submetrics: Dict[str, Metric],
        allowed_exif_tags: Optional[Set[str]] = None,
        forbidden_exif_tags: Optional[Set[str]] = None,
    ):
        if ImageType is None:
            logger.error("Install Pillow for image support")
        super(ImageMetric, self).__init__(submetrics)
        self._allowed_exif_tags = allowed_exif_tags or set()
        self._forbidden_exif_tags = forbidden_exif_tags or set()

    @property
    def namespace(self) -> str:
        return "image"

    def _wants_to_track(self, exif_tag: str) -> bool:
        if exif_tag in self._forbidden_exif_tags:
            return False
        if not self._allowed_exif_tags:  # empty set -> allow anything not explicitly forbidden
            return True
        return exif_tag in self._allowed_exif_tags

    def columnar_update(self, view: PreprocessedColumn) -> OperationResult:
        submetric_names = self.submetrics.keys()
        count = 0
        for image in list(chain.from_iterable(view.raw_iterator())):
            if isinstance(image, ImageType):
                metadata = get_pil_image_metadata(image)
                for attr in metadata.keys():
                    if attr not in submetric_names:  # EXIF tag discovery
                        if self._wants_to_track(attr):
                            # TODO: add a resolver to ImageMetricConfig
                            if isinstance(metadata[attr], int) or isinstance(metadata[attr], float):
                                self.submetrics[attr] = DistributionMetric.zero(MetricConfig())
                            elif isinstance(metadata[attr], str):
                                self.submetrics[attr] = FrequentItemsMetric.zero(MetricConfig())

                    if attr in submetric_names:
                        self.submetrics[attr].columnar_update(PreprocessedColumn.apply([metadata[attr]]))

                count += 1
        return OperationResult.ok(count)

    @classmethod
    def zero(cls, config: MetricConfig) -> "ImageMetric":
        if not isinstance(config, ImageMetricConfig):
            logger.warning("ImageMetric.zero() needs an ImageMetricConfig")
            config = ImageMetricConfig()

        dummy_image = new_image("HSV", (1, 1))
        metadata = get_pil_image_metadata(dummy_image)  # any EXIF tags will be discovered as images are logged
        submetrics: Dict[str, Metric] = dict()
        for tag, value in metadata.items():
            if isinstance(value, int):
                submetrics[tag] = DistributionMetric.zero(config)
            elif isinstance(value, float):
                submetrics[tag] = DistributionMetric.zero(config)
            elif isinstance(value, str) and not config.fi_disabled:
                submetrics[tag] = FrequentItemsMetric.zero(config)

        return ImageMetric(submetrics, config.allowed_exif_tags, config.forbidden_exif_tags)


def log_image(
    images: Union[ImageType, List[ImageType], Dict[str, ImageType]],
    default_column_prefix: str = "image",
    schema: Optional[DatasetSchema] = None,
    default_configs: Optional[ImageMetricConfig] = None,
) -> ResultSet:
    if isinstance(images, ImageType):
        images = {default_column_prefix: images}
    if isinstance(images, list):
        count = 0
        row = dict()
        for img in images:
            row[f"{default_column_prefix}_{count}"] = img
            count += 1
        images = row

    if not isinstance(images, dict):
        raise ValueError("log_image must be passed an image, list of images, or dictionary of images")
    for img in images.values():
        if not isinstance(img, ImageType):
            raise ValueError("log_image must be passed an image, list of images, or dictionary of images")

    class ImageResolver(Resolver):
        def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
            return {ImageMetric.get_namespace(ImageMetricConfig()): ImageMetric.zero(column_schema.cfg)}

    config = default_configs or ImageMetricConfig()
    schema = schema or DatasetSchema(
        types={key: ImageType for key in images.keys()}, default_configs=config, resolvers=ImageResolver()
    )
    return why.log(row=images, schema=schema)
