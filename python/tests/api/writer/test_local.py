import os
import shutil

import pytest

from whylogs.api.writer.local import LocalWriter


class TestLocalWriter(object):
    @classmethod
    def teardown_class(cls):
        shutil.rmtree(os.path.join(os.getcwd(), "html_reports/"))
        shutil.rmtree("test_dir/")

    @pytest.fixture
    def local_writer(self):
        writer = LocalWriter(base_dir="test_dir", base_name="test_name.bin")
        return writer

    def test_should_write_to_default_dir_if_dest_is_none(self, local_writer, profile_view):
        success, files = local_writer.write(file=profile_view, dest=None)
        assert success
        assert os.path.isfile("test_dir/test_name.bin")
        try:
            os.remove("test_dir/test_name.bin")
        except Exception:
            pass

    def test_should_write_to_defined_destination(self, local_writer, profile_view):
        success, files = local_writer.write(file=profile_view, base_dir="test_dir/some_dir")
        assert success
        assert os.path.isfile("test_dir/some_dir/test_name.bin")
        try:
            os.remove("test_dir/some_dir/test_name.bin")
        except Exception:
            pass

    def test_should_write_html_report_locally(self, html_report):
        html_report.writer("local").write()
        assert os.path.isfile(os.path.join(os.getcwd(), "html_reports/ProfileReport.html"))
