import os
import shutil
from unittest.mock import MagicMock

import pytest

from whylogs import DatasetProfileView
from whylogs.api.writer.local import LocalWriter


class TestLocalWriter(object):
    @classmethod
    def teardown_class(cls):
        shutil.rmtree(os.path.join(os.getcwd(), "html_reports/"))

    @pytest.fixture
    def local_writer(self):
        writer = LocalWriter(base_dir="test_dir", base_name="test_name")
        return writer

    @pytest.fixture
    def mocked_profile_view(self):
        profile_view = MagicMock(spec=DatasetProfileView)
        profile_view.write = MagicMock()
        profile_view.creation_timestamp = MagicMock()
        return profile_view

    def test_should_write_to_default_dir_if_dest_is_none(self, local_writer, mocked_profile_view):
        local_writer.write(profile=mocked_profile_view, dest=None)
        mocked_profile_view.write.assert_called_once_with(
            f"test_dir/test_name_{mocked_profile_view.creation_timestamp}.bin"
        )

    def test_should_write_to_defined_destination(self, local_writer, mocked_profile_view):
        local_writer.write(profile=mocked_profile_view, dest="some_dest.bin")
        mocked_profile_view.write.assert_called_once_with("test_dir/some_dest.bin")

    def test_should_write_html_report_locally(self, html_report):
        html_report.writer("local").write()
        assert os.path.isfile(os.path.join(os.getcwd(), "html_reports/ProfileViz.html"))
