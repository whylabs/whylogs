from tempfile import NamedTemporaryFile

import pytest

from whylogs.api.logger.result_set import ViewResultSet
from whylogs.api.reader.local import LocalReader
from whylogs.core.view.dataset_profile_view import DatasetProfileView


@pytest.fixture
def reader():
    return LocalReader()


@pytest.fixture(scope="session")
def file_path(profile_view):
    tmp_file = NamedTemporaryFile()
    profile_view.write(file=tmp_file)
    tmp_file.flush()
    yield tmp_file.name
    tmp_file.close()


def test_local_reader(reader, file_path):
    profile = reader.read(path=file_path)

    assert isinstance(profile, ViewResultSet)
    assert isinstance(profile.view(), DatasetProfileView)

    profile_option = reader.option(path=file_path).read()

    assert isinstance(profile_option, ViewResultSet)
    assert isinstance(profile_option.view(), DatasetProfileView)


def test_empty_path_read(reader):
    with pytest.raises(ValueError) as e:
        reader.read()
    assert e.value.args[0] == "You must define a path to read your file from!"
