import configparser
import os

from whylogs.api.whylabs.auth_file import (
    _create_blank_auth_file,
    get_auth_file_path,
    get_configparser_object,
)


def test_create_blank_auth_file(auth_path):
    _create_blank_auth_file(auth_path=auth_path)
    assert os.path.isfile(auth_path)
    config = configparser.ConfigParser()
    config.read(auth_path)

    assert config["whylabs"] == {}


def test_get_configparser_object(auth_path):
    _create_blank_auth_file(auth_path)

    expected_config = configparser.ConfigParser()
    expected_config.read(auth_path)

    actual_config = get_configparser_object(auth_path)

    assert actual_config == expected_config


def test_get_auth_file_path(auth_path):
    assert not os.path.isfile(auth_path)

    expected_path = get_auth_file_path(auth_path)

    assert os.path.isfile(expected_path)
    assert expected_path == auth_path
