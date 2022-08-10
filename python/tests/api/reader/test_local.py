from tempfile import NamedTemporaryFile

import pytest

from whylogs.api.logger.result_set import ResultSet
from whylogs.api.reader.local import LocalReader
from whylogs.core.view.dataset_profile_view import DatasetProfileView


@pytest.fixture
def file_path(profile_view):
    tmp_file = NamedTemporaryFile()
    profile_view.write(path=tmp_file.name)
    yield tmp_file.name
    tmp_file.close()


@pytest.fixture
def reader():
    return LocalReader()


def test_local_reader(reader, file_path):
    profile = reader.read(path=file_path)

    assert isinstance(profile, ResultSet)
    assert isinstance(profile.view(), DatasetProfileView)

    profile_option = reader.option(path=file_path).read()

    assert isinstance(profile_option, ResultSet)
    assert isinstance(profile_option.view(), DatasetProfileView)


def test_empty_path_read(reader):
    with pytest.raises(ValueError) as e:
        reader.read()
    assert e.value.args[0] == "You must define a path to read your file from!"
