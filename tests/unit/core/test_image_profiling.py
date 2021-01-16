
from uuid import uuid4
import datetime

from whylogs.core.datasetprofile import DatasetProfile, array_profile, dataframe_profile
from whylogs.core.image_profiling import TrackImage, _METADATA_DEFAULT_ATTRIBUTES
from whylogs.features import _IMAGE_FEATURES
import os
from PIL import Image

TEST_DATA_PATH = os.path.abspath(os.path.join(os.path.realpath(
    os.path.dirname(__file__)), os.pardir, os.pardir, os.pardir, "testdata"))


def test_track_image():
    from whylogs.proto import InferredType

    Type = InferredType.Type

    now = datetime.datetime.utcnow()
    shared_session_id = uuid4().hex
    num_image_features = len(_IMAGE_FEATURES)
    num_metadata_features = len(_METADATA_DEFAULT_ATTRIBUTES)

    test_image_path = os.path.join(
        TEST_DATA_PATH, "images", "flower2.jpg")

    total_default_features = num_image_features + num_metadata_features
    profile_1 = DatasetProfile(name="test",
                               session_id=shared_session_id,
                               session_timestamp=now,
                               tags={"key": "value"},
                               metadata={"key": "x1"},)
    trackImage = TrackImage(test_image_path)

    trackImage(profile_1)
    columns = profile_1.columns
    assert len(columns) == total_default_features
    assert columns["Saturation"].number_tracker.count == 67500

    assert columns["BitsPerSample"].counters.count == 3
    assert columns["BitsPerSample"].number_tracker.counters.ints.count == 3
