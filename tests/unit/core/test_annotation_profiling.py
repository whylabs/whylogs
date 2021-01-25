
from uuid import uuid4
import datetime

from whylogs.core.datasetprofile import DatasetProfile, array_profile, dataframe_profile
from whylogs.core.annotation_profiling import TrackBB

import os

TEST_DATA_PATH = os.path.abspath(os.path.join(os.path.realpath(
    os.path.dirname(__file__)), os.pardir, os.pardir, os.pardir, "testdata"))


def test_track_bb_annotation():

    now = datetime.datetime.utcnow()
    shared_session_id = uuid4().hex

    test_annotation_path = os.path.join(
        TEST_DATA_PATH, "files", "yolo_bounding_box.jsonl")

    # total_default_features = num_image_features + num_metadata_features
    profile_1 = DatasetProfile(name="test",
                               session_id=shared_session_id,
                               session_timestamp=now,
                               tags={"key": "value"},
                               metadata={"key": "x1"},)
    trackImage = TrackBB(test_annotation_path)

    # trackImage(profile_1)
    # columns = profile_1.columns
    # assert len(columns) == total_default_features
    # assert columns["Saturation"].number_tracker.count == 67500
    # assert columns["BitsPerSample"].counters.count == 3
    # trackImage = TrackImage(test_image_path, metadata_attributes="all")
    # trackImage(profile_1)
    # columns = profile_1.columns
    # assert len(columns) == total_default_features+11
    # assert columns["Saturation"].number_tracker.count == 2*67500


# def test_track_PIL_img():
#     from whylogs.proto import InferredType

#     Type = InferredType.Type

#     now = datetime.datetime.utcnow()
#     shared_session_id = uuid4().hex
#     num_image_features = len(_IMAGE_FEATURES)
#     num_metadata_features = len(_METADATA_DEFAULT_ATTRIBUTES)

#     test_image_path = os.path.join(
#         TEST_DATA_PATH, "images", "flower2.jpg")

#     total_default_features = num_image_features + num_metadata_features
#     profile_1 = DatasetProfile(name="test",
#                                session_id=shared_session_id,
#                                session_timestamp=now,
#                                tags={"key": "value"},
#                                metadata={"key": "x1"},)
#     img = Image.open(open(test_image_path, "rb"))
#     trackImage = TrackImage(img=img)

#     trackImage(profile_1)
#     columns = profile_1.columns
#     assert len(columns) == total_default_features
#     assert columns["Saturation"].number_tracker.count == 67500
#     assert columns["BitsPerSample"].counters.count == 3
