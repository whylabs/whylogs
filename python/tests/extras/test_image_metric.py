import logging
import os
from typing import Dict

import whylogs as why
from whylogs.core import DatasetProfileView
from whylogs.core.configs import SummaryConfig
from whylogs.core.datatypes import DataType
from whylogs.core.metrics import Metric
from whylogs.core.preprocessing import ListView, PreprocessedColumn
from whylogs.core.resolvers import Resolver
from whylogs.core.schema import ColumnSchema, DatasetSchema
from whylogs.extras.image_metric import (
    ImageMetric,
    ImageMetricConfig,
    init_image_schema,
    log_image,
)

logger = logging.getLogger(__name__)

try:
    import numpy as np
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
    assert summary["ImagePixelWidth:distribution/mean"] > 0
    assert summary["ImagePixelWidth:distribution/n"] == 1
    # these should be discovered EXIF tags
    assert summary["ImageWidth:distribution/max"] == 1733
    assert summary["PhotometricInterpretation:distribution/min"] == 2
    assert summary["Orientation:distribution/max"] == 1
    assert summary["ResolutionUnit:distribution/max"] == 2
    assert "Software:frequent_items/frequent_strings" in summary

    # resolved int submetrics
    for namespace in ["counts", "types", "cardinality", "distribution", "ints", "frequent_items"]:
        assert namespace in metric.submetrics["ImagePixelWidth"].keys()
    assert summary["ImagePixelWidth:types/integral"] == summary["ImagePixelWidth:distribution/n"]

    for component in ["fractional", "boolean", "string", "object"]:
        assert summary[f"ImagePixelWidth:types/{component}"] == 0

    for tag in ["ImagePixelWidth", "PhotometricInterpretation", "Orientation", "ResolutionUnit"]:
        assert summary[f"{tag}:counts/n"] == summary[f"{tag}:distribution/n"]
        assert summary[f"{tag}:counts/null"] == 0

    # resolved string submetrics
    for namespace in ["counts", "types", "cardinality", "frequent_items"]:
        assert namespace in metric.submetrics["Software"]
    assert "distribution" not in metric.submetrics["Software"]
    assert "ints" not in metric.submetrics["Software"]


def test_log_np_image() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = np.array(image_loader(image_path))

    schema = init_image_schema()
    profile = why.log({"image": img}, schema=schema)
    df = profile.profile().view().to_pandas()

    # Ensure a few columns are in the data frame from the image metric
    assert "image/Brightness.mean:cardinality/est" in df.columns
    assert "image/Brightness.mean:cardinality/lower_1" in df.columns
    assert "image/entropy:types/tensor" in df.columns


def test_allowed_exif_tags() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    ppc = PreprocessedColumn()
    ppc.list = ListView(objs=[img])
    config = ImageMetricConfig(allowed_exif_tags={"PhotometricInterpretation"})
    metric = ImageMetric.zero(config)
    metric.columnar_update(ppc)
    summary = metric.to_summary_dict(SummaryConfig())
    assert summary["ImagePixelWidth:distribution/mean"] > 0  # image stat, not an EXIF tag
    assert "PhotometricInterpretation:distribution/min" in summary  # allowed EXIF tag
    assert "ResolutionUnit:distribution/max" not in summary  # not an allowed EXIF tag


def test_forbidden_exif_tags() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    ppc = PreprocessedColumn()
    ppc.list = ListView(objs=[img])
    config = ImageMetricConfig(forbidden_exif_tags={"PhotometricInterpretation"})
    metric = ImageMetric.zero(config)
    metric.columnar_update(ppc)
    summary = metric.to_summary_dict(SummaryConfig())
    assert summary["ImagePixelWidth:distribution/mean"] > 0  # image stat, not an EXIF tag
    assert "PhotometricInterpretation:distribution/min" not in summary  # forbidden EXIF tag
    assert (
        "ResolutionUnit:distribution/max" in summary
    )  # empty allowed_exif_tags means allow anything not explicitly forbidden


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
    assert summary["ImagePixelWidth:distribution/mean"] > 0  # image stat, not an EXIF tag
    assert "PhotometricInterpretation:distribution/min" not in summary  # forbidden > allowed
    assert (
        "ResolutionUnit:distribution/max" not in summary
    )  # non-empty allowed_exif_tags means only explicitly allowed tags


def test_log_image() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    results = log_image(img).view()
    logger.info(results.get_column("image").to_summary_dict())
    assert results.get_column("image").to_summary_dict()["image/ImagePixelWidth:distribution/mean"] > 0


def test_log_image_with_entropy() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    results = log_image(img).view()
    logger.info(results.get_column("image").to_summary_dict())
    assert results.get_column("image").to_summary_dict()["image/entropy:distribution/mean"] > 0


def test_log_interface() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)

    schema = DatasetSchema(default_configs=ImageMetricConfig(), resolvers=TestResolver())

    results = why.log(row={"image_col": img}, schema=schema).view().get_column("image_col")
    logger.info(results.to_summary_dict())
    assert results.to_summary_dict()["image/ImagePixelWidth:distribution/mean"] > 0


def test_deserialize_profile(tmp_path) -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    profile_view = log_image(img, "image_column").view()
    profile_view._write(path=tmp_path, filename="test.bin")
    deserialized_view = DatasetProfileView.read(os.path.join(tmp_path, "test.bin"))
    assert (
        profile_view.get_column("image_column").to_summary_dict()
        == deserialized_view.get_column("image_column").to_summary_dict()
    )
