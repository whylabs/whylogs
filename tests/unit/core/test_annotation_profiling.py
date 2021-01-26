import json
from uuid import uuid4
import datetime

from whylogs.core.datasetprofile import DatasetProfile, array_profile, dataframe_profile
from whylogs.core.annotation_profiling import TrackBB, BB_ATTRIBUTES

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
    trackbb = TrackBB(test_annotation_path)
    trackbb(profile_1)

    columns = profile_1.columns
    assert len(columns) == len(BB_ATTRIBUTES)
    for each_attribute in BB_ATTRIBUTES:
        assert columns.get(each_attribute, None) is not None
        if each_attribute in ("annotation_count", "area_coverage", "annotation_density"):
            assert columns[each_attribute].number_tracker.count == 100
        else:
            assert columns[each_attribute].number_tracker.count == 4183


def test_track_json_annotation():

    now = datetime.datetime.utcnow()
    shared_session_id = uuid4().hex
    num_bb_features = len(BB_ATTRIBUTES)

    test_annotation_path = os.path.join(
        TEST_DATA_PATH, "files", "yolo_bounding_box.jsonl")

    profile_1 = DatasetProfile(name="test",
                               session_id=shared_session_id,
                               session_timestamp=now,
                               tags={"key": "value"},
                               metadata={"key": "x1"},)

    objs = [json.loads(eachline)
            for eachline in open(test_annotation_path, "r")]
    trackbb = TrackBB(obj=objs)

    trackbb(profile_1)
    columns = profile_1.columns
    assert len(columns) == len(BB_ATTRIBUTES)
    assert columns["annotation_count"].number_tracker.count == 100
