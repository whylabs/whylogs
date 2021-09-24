import datetime
import os
from uuid import uuid4

from PIL import Image

from whylogs.core.datasetprofile import DatasetProfile, array_profile, dataframe_profile
from whylogs.core.image_profiling import _METADATA_DEFAULT_ATTRIBUTES, TrackImage
from whylogs.features import _IMAGE_FEATURES

TEST_DATA_PATH = os.path.abspath(
    os.path.join(
        os.path.realpath(os.path.dirname(__file__)),
        os.pardir,
        os.pardir,
        os.pardir,
        "testdata",
    )
)


def test_track_image():
    now = datetime.datetime.now(datetime.timezone.utc)
    shared_session_id = uuid4().hex
    test_image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")

    total_default_features = _METADATA_DEFAULT_ATTRIBUTES

    profile_1 = DatasetProfile(
        name="test",
        session_id=shared_session_id,
        session_timestamp=now,
        tags={"key": "value"},
        metadata={"key": "x1"},
    )
    trackImage = TrackImage(test_image_path)

    trackImage(profile_1)
    columns = profile_1.columns
    for feature_name in total_default_features:
        assert feature_name in columns, f"{feature_name} not in {columns}"

    assert columns["Saturation.mean"].number_tracker.count == 1
    assert columns["Saturation.stddev"].number_tracker.count == 1
    assert columns["BitsPerSample"].counters.count == 1

    trackImage = TrackImage(test_image_path)
    trackImage(profile_1)
    columns = profile_1.columns
    for feature_name in total_default_features:
        assert feature_name in columns, f"{feature_name} not in {columns}"

    assert columns["Saturation.mean"].number_tracker.count == 2
    assert columns["Saturation.stddev"].number_tracker.count == 2
    assert columns["BitsPerSample"].counters.count == 2


def test_track_PIL_img():
    now = datetime.datetime.now(datetime.timezone.utc)
    shared_session_id = uuid4().hex
    total_default_features = _METADATA_DEFAULT_ATTRIBUTES

    test_image_path = os.path.join(TEST_DATA_PATH, "images", "flower2.jpg")

    profile_1 = DatasetProfile(
        name="test",
        session_id=shared_session_id,
        session_timestamp=now,
        tags={"key": "value"},
        metadata={"key": "x1"},
    )
    img = Image.open(open(test_image_path, "rb"))
    trackImage = TrackImage(img=img)

    trackImage(profile_1)
    columns = profile_1.columns
    for feature_name in total_default_features:
        assert feature_name in columns, f"{feature_name} not in {columns}"
    assert columns["Saturation.mean"].number_tracker.count == 1
    assert columns["BitsPerSample"].counters.count == 1
