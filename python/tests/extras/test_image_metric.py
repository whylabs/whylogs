
import logging
import os
from typing import Dict

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


import whylogs as why
from whylogs.api.writers.whylabs import _uncompund_dataset_profile
from whylogs.core.configs import SummaryConfig
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.datatypes import DataType
from whylogs.core.metrics import Metric, MetricConfig
from whylogs.core.metrics.unicode_range import _STRING_LENGTH, UnicodeRangeMetric
from whylogs.core.preprocessing import ListView, PreprocessedColumn
from whylogs.core.resolvers import Resolver
from whylogs.core.schema import ColumnSchema, DatasetSchema

from whylogs.extras.image_metric import (
    ImageMetric, 
    get_pil_image_statistics,
    get_pil_image_metadata,
    image_based_metadata,
    log_image,
)



TEST_DATA_PATH = os.path.abspath(
    os.path.join(
        os.path.realpath(os.path.dirname(__file__)),
        os.pardir,
        "testdata",
    )
)


def image_loader(path: str = None) -> ImageType:
    from PIL import Image  # to throw if PIL's not available

    with open(path, "rb") as file_p:
        img = Image.open(file_p).copy()
        return img


class TestResolver(Resolver):
    def resolve(self, name: str, why_type: DataType, column_schema: ColumnSchema) -> Dict[str, Metric]:
        return {ImageMetric.get_namespace(MetricConfig()): ImageMetric.zero(column_schema.cfg)}


def test_image_metric() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    ppc = PreprocessedColumn()
    ppc.list = ListView(objs=[img])
    metric = ImageMetric.zero(MetricConfig())
    metric.columnar_update(ppc)
    assert metric.to_summary_dict(None)["image/ImagePixelWidth/mean"] > 0


def test_log_image() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    results = log_image(img).view()
    print(results.get_column("image").to_summary_dict())
    assert results.get_column("image").to_summary_dict()["image/image/ImagePixelWidth/mean"] > 0


def test_log_interface() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)

    schema = DatasetSchema(resolvers=TestResolver())

    results = why.log(row={"image_col": img}, schema=schema).view().get_column("image_col")
    print(results.to_summary_dict())
    assert results.to_summary_dict()["image/image/ImagePixelWidth/mean"] > 0


def test_uncompound_profile() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    profile_view = log_image(img, "image_column").view()
    uncompounded = _uncompund_dataset_profile(profile_view)
    assert "image_column" in uncompounded._columns
    assert "image" in uncompounded._columns["image_column"].metrics  # original compound metric
    assert "image_column.image.ImagePixelWidth" in uncompunded._columns
    assert "distribution" in uncompoounded._columns["image_column.image.ImagePixelWidth"].metrics  # uncompounded
