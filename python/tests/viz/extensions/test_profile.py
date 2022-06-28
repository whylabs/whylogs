import os
import tempfile

import pytest

from whylogs.viz.extensions.viz_profile_writer import VizProfileWriter


class TestVizProfileWriter(object):
    @pytest.fixture
    def rendered_html(self):
        rendered_html = "<div></div>"
        return rendered_html

    def test_viz_profile_injected_with_html_object(self, rendered_html):
        viz_profile = VizProfileWriter(rendered_html=rendered_html)
        assert isinstance(viz_profile.rendered_html, str)
        assert viz_profile.rendered_html == "<div></div>"


    def test_viz_profile_writes_to_path(self, rendered_html):
        with tempfile.TemporaryDirectory() as path:
            viz_profile = VizProfileWriter(rendered_html=rendered_html)
            viz_profile.write(path=path)
            assert os.path.isfile(os.path.join(path, "ProfileVisualizer.html"))
            assert os.path.isdir(path)

    def test_viz_profile_writes_with_file_name(self, rendered_html):
        with tempfile.TemporaryDirectory() as path:
            viz_profile = VizProfileWriter(rendered_html)
            viz_profile.write(path=path, file_name="MyFile")
            assert os.path.isfile(os.path.join(path, "MyFile.html"))
            assert os.path.isdir(path)
