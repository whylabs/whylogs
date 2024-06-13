import os
import pandas as pd

import whylogs as why

from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.schema import DatasetSchema
from whylogs.core.segmentation_partition import segment_on_column
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView
from whylogs.viz.extensions.reports.profile_summary import ProfileSummaryReport


def test_dataset_profile_write(tmp_path):
    profile = why.log({"a": 1}).profile()
    path = os.path.join(tmp_path, "test_dataset_profile_write.bin")
    success, files = profile.write(path)
    assert success
    assert path in files
    assert os.path.isfile(path)


def test_dataset_profile_view_write(tmp_path):
    view = why.log({"a": 1}).profile().view()
    path = os.path.join(tmp_path, "test_dataset_profile_view_write.bin")
    success, files = view.write(path)
    assert success
    assert path in files
    assert os.path.isfile(path)


def test_segmented_dataset_profile_view_write(tmp_path):
    df = pd.DataFrame({"a": [1, 2]})
    schema = DatasetSchema(segments=segment_on_column("a"))
    results = why.log(df, schema=schema)
    writables = results.get_writables()
    assert isinstance(writables[0], SegmentedDatasetProfileView)
    path = os.path.join(tmp_path, "test_segmented_dataset_profile_view_write.bin")
    success, files = writables[0].write(path)
    assert success
    assert path in files
    assert os.path.isfile(path)


def test_html_report_write(tmp_path):
    view = why.log({"a": 1}).profile().view()
    report = ProfileSummaryReport(view)
    path = os.path.join(tmp_path, "test_html_report_write.bin")
    success, files = view.write(path)
    assert success
    assert path in files
    assert os.path.isfile(path)
