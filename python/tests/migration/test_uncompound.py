import os

import pandas as pd
from PIL import Image

import whylogs as why
import whylogs.migration.uncompound as uc
from whylogs.core.metrics.condition_count_metric import (
    Condition,
    ConditionCountConfig,
    ConditionCountMetric,
)
from whylogs.core.relations import Predicate
from whylogs.core.resolvers import MetricSpec, ResolverSpec
from whylogs.core.schema import DeclarativeSchema
from whylogs.extras.image_metric import log_image
from whylogs.migration.uncompound import (
    _condition_count_magic_string,
    _uncompound_dataset_profile,
)

X = Predicate()


# Turn on uncompounding of CounditionCountMetric
uc._uncompound_condition_count_feature_flag = lambda *args, **kwargs: True


try:
    from PIL.Image import Image as ImageType
except ImportError:
    ImageType = None


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


def test_uncompound_profile() -> None:
    image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")
    img = image_loader(image_path)
    profile_view = log_image(img, "image_column").view()
    uncompounded = _uncompound_dataset_profile(profile_view)
    assert "image_column" in uncompounded._columns
    assert "image" in uncompounded._columns["image_column"]._metrics  # original compound metric
    assert "image_column.ImagePixelWidth" in uncompounded._columns

    for metric in ["counts", "types", "cardinality", "distribution", "ints", "frequent_items"]:
        assert metric in uncompounded._columns["image_column.ImagePixelWidth"]._metrics  # uncompounded

    for metric in ["counts", "types", "cardinality", "frequent_items"]:
        assert metric in uncompounded._columns["image_column.Software"]._metrics


def test_uncompounded_image_profile() -> None:
    img1 = Image.effect_mandelbrot((256, 256), (-3, -2.5, 2, 2.5), 9)
    img2 = Image.effect_mandelbrot((256, 256), (-3, -2.5, 2, 2.5), 20)

    prof1 = log_image(img1).profile().view()
    prof2 = log_image(img2).profile().view()
    merged_prof = prof1.merge(prof2)

    assert set(prof1.get_column("image")._metrics["image"].submetrics.keys()) == set(
        prof2.get_column("image")._metrics["image"].submetrics.keys()
    )
    assert set(merged_prof.get_column("image")._metrics["image"].submetrics.keys()) == set(
        prof2.get_column("image")._metrics["image"].submetrics.keys()
    )
    uncomp1 = _uncompound_dataset_profile(prof1)
    uncomp2 = _uncompound_dataset_profile(prof2)
    uncompM = _uncompound_dataset_profile(merged_prof)
    assert set(uncomp1.get_columns()) == set(uncomp2.get_columns())
    assert set(uncomp1.get_columns()) == set(uncompM.get_columns())
    assert prof1.to_pandas().shape == uncomp1.to_pandas().shape


def test_uncompounded_profile_metadata() -> None:
    results = why.log({"a": 1.2})
    view = results.view()
    test_key = "test_key"
    test_value = "metadata value"
    test_metadata = {test_key: test_value}
    assert view is not None
    view._metadata = test_metadata
    uncompounded = _uncompound_dataset_profile(view)
    assert uncompounded is not None
    assert uncompounded._metadata is not None
    assert test_key in uncompounded._metadata
    assert view._metadata[test_key] == test_value


def test_uncompounded_condition_count() -> None:
    conditions = {
        "alpha": Condition(X.matches("[a-zA-Z]+")),
        "digit": Condition(X.matches("[0-9]+")),
    }
    config = ConditionCountConfig(conditions=conditions)
    schema = DeclarativeSchema(
        [
            ResolverSpec(
                column_name="col1",
                metrics=[
                    MetricSpec(
                        ConditionCountMetric,
                        config,
                    ),
                ],
            ),
        ]
    )

    df = pd.DataFrame({"col1": ["abc", "123"]})
    profile = why.log(df, schema=schema).profile().view()
    metric = profile.get_column("col1")._metrics["condition_count"]
    assert metric.total.value == 2
    assert metric.matches["alpha"].value == 1
    assert metric.matches["digit"].value == 1
    uncompounded = _uncompound_dataset_profile(profile)
    assert len(uncompounded.get_columns().keys()) == 1 + 3 * len(metric.matches.keys())
    for cond_name in ["alpha", "digit"]:
        for component_name in ["total", "matches", "non_matches"]:
            column_name = f"{_condition_count_magic_string()}col1.{cond_name}.{component_name}"
            metric = uncompounded._columns[column_name]._metrics["counts"]
            if component_name == "total":
                assert metric.n.value == 2
            else:
                assert metric.n.value == 1
            assert metric.null.value == 0
            assert metric.nan.value == 0
            assert metric.inf.value == 0
