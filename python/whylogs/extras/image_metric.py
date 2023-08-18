import json
import logging
import time
from copy import deepcopy
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Dict, List, Optional, Set, Union
from uuid import uuid4

import whylogs as why
from whylogs.api.logger.result_set import ResultSet
from whylogs.core.datatypes import (
    DataType,
    Fractional,
    Integral,
    StandardTypeMapper,
    String,
    TypeMapper,
)
from whylogs.core.metrics import StandardMetric
from whylogs.core.metrics.metrics import (
    Metric,
    MetricConfig,
    OperationResult,
    register_metric,
)
from whylogs.core.metrics.multimetric import MultiMetric, SubmetricSchema
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.resolvers import Resolver
from whylogs.core.schema import ColumnSchema, DatasetSchema

logger = logging.getLogger(__name__)

try:
    from PIL.Image import Image as ImageType
    from PIL.ImageStat import Stat
    from PIL.TiffImagePlugin import IFDRational
    from PIL.TiffTags import TAGS
except ImportError as e:
    ImageType = None  # type: ignore
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
    if hasattr(img, "entropy"):
        metadata["entropy"] = img.entropy()
    for index in range(len(channels)):
        for statistic_name in image_stats:
            if hasattr(stats, statistic_name):
                metadata[channels[index] + "." + statistic_name] = getattr(stats, statistic_name)[index]

    return metadata


def get_pil_exif_metadata(img: ImageType) -> Dict:
    """
    Grab EXIF metadata from image

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

    return metadata


def image_based_metadata(img):
    return {
        "ImagePixelWidth": img.width,
        "ImagePixelHeight": img.height,
        "Colorspace": img.mode,
    }


def get_pil_image_metadata(img: ImageType) -> Dict:
    """
    Grab statistics data from a PIL ImageStats.Stat

    Args:
        img (ImageType): PIL Image

    Returns:
        Dict: of metadata
    """
    metadata = {}
    metadata.update(image_based_metadata(img))
    metadata.update(get_pil_image_statistics(img))
    return metadata


class ImageSubmetricSchema(SubmetricSchema):
    def resolve(self, name: str, why_type: DataType, fi_disabled: bool = False) -> Dict[str, Metric]:
        metrics: Dict[str, Metric] = {
            "counts": StandardMetric.counts.zero(MetricConfig()),
            "types": StandardMetric.types.zero(MetricConfig()),
            "cardinality": StandardMetric.cardinality.zero(MetricConfig()),
        }

        if isinstance(why_type, Integral):
            metrics["distribution"] = StandardMetric.distribution.zero(MetricConfig())
            metrics["ints"] = StandardMetric.ints.zero(MetricConfig())
            if not fi_disabled:
                metrics["frequent_items"] = StandardMetric.frequent_items.zero(MetricConfig())
        elif isinstance(why_type, Fractional):
            metrics["distribution"] = StandardMetric.distribution.zero(MetricConfig())
        elif isinstance(why_type, String) and not fi_disabled:
            metrics["frequent_items"] = StandardMetric.frequent_items.zero(MetricConfig())

        return metrics


@dataclass(frozen=True)
class ImageMetricConfig(MetricConfig):
    allowed_exif_tags: Set[str] = field(default_factory=set)
    forbidden_exif_tags: Set[str] = field(default_factory=set)


class ImageMetric(MultiMetric):
    def __init__(
        self,
        submetrics: Dict[str, Dict[str, Metric]],
        allowed_exif_tags: Optional[Set[str]] = None,
        forbidden_exif_tags: Optional[Set[str]] = None,
        submetric_schema: Optional[SubmetricSchema] = None,
        type_mapper: Optional[TypeMapper] = None,
        fi_disabled: bool = False,
    ):
        if ImageType is None:
            logger.error("Install Pillow for image support")
        super(ImageMetric, self).__init__(submetrics)
        self._allowed_exif_tags = allowed_exif_tags or set()
        self._forbidden_exif_tags = forbidden_exif_tags or set()
        self._submetric_schema = submetric_schema or ImageSubmetricSchema()
        self._type_mapper = type_mapper or StandardTypeMapper()
        self._fi_disabled = fi_disabled

    @property
    def namespace(self) -> str:
        return "image"

    def merge(self, other: "ImageMetric") -> "ImageMetric":
        merged = super(ImageMetric, self).merge(other)
        merged._allowed_exif_tags = self._allowed_exif_tags.copy()
        merged._forbidden_exif_tags = self._forbidden_exif_tags.copy()
        merged._submetric_schema = deepcopy(self._submetric_schema)
        merged._type_mapper = deepcopy(self._type_mapper)
        merged._fi_disabled = self._fi_disabled
        return merged

    def _wants_to_track(self, exif_tag: str) -> bool:
        if exif_tag in self._forbidden_exif_tags:
            return False
        if not self._allowed_exif_tags:  # empty set -> allow anything not explicitly forbidden
            return True
        return exif_tag in self._allowed_exif_tags

    def _add_submetric(self, name: str, value: Any) -> None:
        self.submetrics[name] = self._submetric_schema.resolve(name, self._type_mapper(type(value)), self._fi_disabled)

    def _discover_exif_submetrics(self, name: str, value: Any) -> None:
        if name not in self.submetrics and self._wants_to_track(name):
            self._add_submetric(name, value)

    def _discover_image_submetrics(self, name: str, value: Any) -> None:
        if name not in self.submetrics:
            self._add_submetric(name, value)

    def _update_relevant_submetrics(self, name: str, data: PreprocessedColumn) -> None:
        if name not in self.submetrics:
            return

        for metric in self.submetrics[name].values():
            metric.columnar_update(data)

    def columnar_update(self, view: PreprocessedColumn) -> OperationResult:
        count = 0
        for image in list(chain.from_iterable(view.raw_iterator())):
            if isinstance(image, ImageType):
                metadata = get_pil_exif_metadata(image)
                for name, value in metadata.items():
                    self._discover_exif_submetrics(name, value)  # EXIF tag discovery
                    data = PreprocessedColumn.apply([metadata[name]])  # TODO: _process_scalar_value()?
                    self._update_relevant_submetrics(name, data)

                image_data = get_pil_image_metadata(image)
                for name, value in image_data.items():
                    self._discover_image_submetrics(name, value)
                    data = PreprocessedColumn.apply([image_data[name]])
                    self._update_relevant_submetrics(name, data)

                count += 1
        return OperationResult.ok(count)

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "ImageMetric":
        config = config or ImageMetricConfig()
        if not isinstance(config, ImageMetricConfig):
            logger.error("ImageMetric.zero() needs an ImageMetricConfig")
            config = ImageMetricConfig()

        return ImageMetric(
            {},  # submetrics will be discovered as images are logged
            config.allowed_exif_tags,
            config.forbidden_exif_tags,
            None,  # use standard ImageSubmetricSchema
            None,  # use standard TypeMapper
            config.fi_disabled,
        )


def log_image(
    images: Union[ImageType, List[ImageType], Dict[str, ImageType]],
    default_column_prefix: str = "image",
    schema: Optional[DatasetSchema] = None,
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
            return {ImageMetric.get_namespace(): ImageMetric.zero(column_schema.cfg)}

    schema = schema or DatasetSchema(
        types={key: ImageType for key in images.keys()}, default_configs=ImageMetricConfig(), resolvers=ImageResolver()
    )
    if not isinstance(schema.default_configs, ImageMetricConfig):
        raise ValueError("log_image requires DatasetSchema with an ImageMetricConfig as default_configs")

    return why.log(row=images, schema=schema)


def _get_timed_file_name(
    trace_id: str,
    timestamp: Optional[float] = None,
    column_name: Optional[str] = None,
    prefix: str = "debug_event_",
    format: str = "%Y-%m-%d_%H-%M",
) -> str:
    if not timestamp:
        time_tuple = time.gmtime(time.time())
    else:
        time_tuple = time.gmtime(timestamp)

    feature_name = f"{column_name}_" if column_name else ""

    timed_filename = f"{prefix}{feature_name}{trace_id}.{time.strftime(format, time_tuple)}.json"
    return timed_filename


# put the segment in here, tags
def log_debug_event(
    column_name: Optional[str] = None,
    trace_id: Optional[str] = None,
    debug_event: Optional[Dict[str, Any]] = None,
    tags: Optional[List[str]] = None,
    timestamp: Optional[float] = None,
    segment_key_values: Optional[Dict[str, str]] = None,  # can be used to partition the data
):
    if not trace_id:
        trace_id = str(uuid4())

    if debug_event is None:
        debug_event = dict()
    debug_event["whylabs.traceId"] = str(trace_id)  # Can be user supplied but needs to be str
    debug_event["whylogs.write_timestamp"] = time.time()
    if timestamp:
        debug_event["whylogs.timestamp"] = timestamp

    if column_name:
        debug_event["whylogs.column_name"] = column_name
    if tags:
        debug_event["whylogs.tags"] = set(tags)
    if segment_key_values:
        debug_event["whylogs.segments"] = segment_key_values

    filename = _get_timed_file_name(trace_id=trace_id, column_name=column_name, timestamp=timestamp)
    # TODO: integrate with DebugEvents service, but for now save the
    # debug_event dictionary as a JSON file
    with open(filename, "w") as json_file:
        json.dump(debug_event, json_file)

    logger.info(f"debug_event saved to {filename}")


def log_single_image(
    image: ImageType,
    column_name: str = "image",
    schema: Optional[DatasetSchema] = None,
    trace_id: Optional[str] = None,
    segment_key_values: Optional[Dict[str, str]] = None,
    debug_event: Optional[Dict[str, Any]] = None,
) -> ResultSet:
    image_message = {column_name: image}

    class ImageResolver(Resolver):
        def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
            return {ImageMetric.get_namespace(): ImageMetric.zero(column_schema.cfg)}

    schema = schema or DatasetSchema(
        types={column_name: ImageType}, default_configs=ImageMetricConfig(), resolvers=ImageResolver()
    )

    if not isinstance(schema.default_configs, ImageMetricConfig):
        raise ValueError("log_single_image requires DatasetSchema with an ImageMetricConfig as default_configs")

    if debug_event:
        if trace_id is None:
            trace_id = str(uuid4())
        log_debug_event(
            column_name=column_name, trace_id=trace_id, debug_event=debug_event, segment_key_values=segment_key_values
        )

    result_set = why.log(row=image_message, schema=schema)
    result_set.metadata["whylabs.traceId"] = trace_id

    return result_set


# Register it so Multimetric and ProfileView can deserialize
register_metric(ImageMetric)
