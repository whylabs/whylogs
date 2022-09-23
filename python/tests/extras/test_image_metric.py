import logging
import os
from typing import Dict

import whylogs as why
from whylogs.api.writer.whylabs import _uncompund_dataset_profile
from whylogs.core.configs import SummaryConfig
from whylogs.core.datatypes import DataType
from whylogs.core.metrics import Metric
from whylogs.core.preprocessing import ListView, PreprocessedColumn
from whylogs.core.resolvers import Resolver
from whylogs.core.schema import ColumnSchema, DatasetSchema
from whylogs.extras.image_metric import ImageMetric, ImageMetricConfig, log_image

logger = logging.getLogger(__name__)

try:
    from PIL.Image import Image as ImageType
except ImportError as e:
    ImageType = None
    logger.debug(str(e))
    logger.debug("Unable to load PIL; install Pillow for image support")


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
        return {ImageMetric.get_namespace(ImageMetricConfig()): ImageMetric.zero(column_schema.cfg)}


def test_image_metric() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    ppc = PreprocessedColumn()
    ppc.list = ListView(objs=[img])
    metric = ImageMetric.zero(ImageMetricConfig())
    metric.columnar_update(ppc)
    summary = metric.to_summary_dict(SummaryConfig())
    assert summary["dist_ImagePixelWidth/mean"] > 0
    assert summary["dist_ImagePixelWidth/n"] == 1
    # these should be discovered EXIF tags
    assert summary["dist_ImageWidth/max"] == 1733
    assert summary["dist_PhotometricInterpretation/min"] == 2
    assert summary["dist_Orientation/max"] == 1
    assert summary["dist_ResolutionUnit/max"] == 2
    assert "fi_Software/frequent_strings" in summary

    # resolved int submetrics
    for prefix in ["counts", "types", "card", "dist", "ints", "fi"]:
        assert f"{prefix}_ImagePixelWidth" in metric.submetrics.keys()
    assert summary["types_ImagePixelWidth/integral"] == summary["dist_ImagePixelWidth/n"]

    for component in ["fractional", "boolean", "string", "object"]:
        assert summary[f"types_ImagePixelWidth/{component}"] == 0

    for tag in ["ImagePixelWidth", "PhotometricInterpretation", "Orientation",  "ResolutionUnit"]:
        assert summary[f"counts_{tag}/n"] == summary[f"dist_{tag}/n"]
        assert summary[f"counts_{tag}/null"] == 0

    # resolved string submetrics
    for prefix in ["counts", "types", "card", "fi"]:
        assert f"{prefix}_Software" in metric.submetrics.keys()
    assert "dist_Software" not in metric.submetrics.keys()
    assert "ints_Software" not in metric.submetrics.keys()


def test_allowed_exif_tags() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    ppc = PreprocessedColumn()
    ppc.list = ListView(objs=[img])
    config = ImageMetricConfig(allowed_exif_tags={"PhotometricInterpretation"})
    metric = ImageMetric.zero(config)
    metric.columnar_update(ppc)
    summary = metric.to_summary_dict(SummaryConfig())
    assert summary["dist_ImagePixelWidth/mean"] > 0  # image stat, not an EXIF tag
    assert "dist_PhotometricInterpretation/min" in summary  # allowed EXIF tag
    assert "dist_ResolutionUnit/max" not in summary  # not an allowed EXIF tag


def test_forbidden_exif_tags() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    ppc = PreprocessedColumn()
    ppc.list = ListView(objs=[img])
    config = ImageMetricConfig(forbidden_exif_tags={"PhotometricInterpretation"})
    metric = ImageMetric.zero(config)
    metric.columnar_update(ppc)
    summary = metric.to_summary_dict(SummaryConfig())
    assert summary["dist_ImagePixelWidth/mean"] > 0  # image stat, not an EXIF tag
    assert "dist_PhotometricInterpretation/min" not in summary  # forbidden EXIF tag
    assert "dist_ResolutionUnit/max" in summary  # empty allowed_exif_tags means allow anything not explicitly forbidden


def test_forbidden_overrules_allowed_exif_tags() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    ppc = PreprocessedColumn()
    ppc.list = ListView(objs=[img])
    config = ImageMetricConfig(
        allowed_exif_tags={"PhotometricInterpretation"}, forbidden_exif_tags={"PhotometricInterpretation"}
    )
    metric = ImageMetric.zero(config)
    metric.columnar_update(ppc)
    summary = metric.to_summary_dict(SummaryConfig())
    assert summary["dist_ImagePixelWidth/mean"] > 0  # image stat, not an EXIF tag
    assert "dist_PhotometricInterpretation/min" not in summary  # forbidden > allowed
    assert "dist_ResolutionUnit/max" not in summary  # non-empty allowed_exif_tags means only explicitly allowed tags


def test_log_image() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    results = log_image(img).view()
    logger.info(results.get_column("image").to_summary_dict())
    assert results.get_column("image").to_summary_dict()["image/dist_ImagePixelWidth/mean"] > 0


def test_log_interface() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)

    schema = DatasetSchema(default_configs=ImageMetricConfig(), resolvers=TestResolver())

    results = why.log(row={"image_col": img}, schema=schema).view().get_column("image_col")
    logger.info(results.to_summary_dict())
    assert results.to_summary_dict()["image/dist_ImagePixelWidth/mean"] > 0


def test_uncompound_profile() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    profile_view = log_image(img, "image_column").view()
    uncompounded = _uncompund_dataset_profile(profile_view)
    assert "image_column" in uncompounded._columns
    assert "image" in uncompounded._columns["image_column"]._metrics  # original compound metric
    assert "image_column.ImagePixelWidth" in uncompounded._columns

    for metric in ["counts", "types", "cardinality", "distribution", "ints", "frequent_items"]:
        assert metric in uncompounded._columns["image_column.ImagePixelWidth"]._metrics  # uncompounded

    for metric in ["counts", "types", "cardinality", "frequent_items"]:
        assert metric in uncompounded._columns["image_column.Software"]._metrics
