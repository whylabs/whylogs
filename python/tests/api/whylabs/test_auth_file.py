import configparser
import os
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from whylogs.api.whylabs.auth_file import (
    _create_blank_auth_file,
    get_auth_file_path,
    get_configparser_object,
)


def test_create_blank_auth_file(auth_path) -> None:
    _create_blank_auth_file(auth_path=auth_path)
    assert os.path.isfile(auth_path)
    config = configparser.ConfigParser()
    config.read(auth_path)

    assert config["whylabs"] == {}


def test_get_configparser_object(auth_path) -> None:
    _create_blank_auth_file(auth_path)

    expected_config = configparser.ConfigParser()
    expected_config.read(auth_path)

    actual_config = get_configparser_object(auth_path)

    assert actual_config == expected_config


def test_get_auth_file_path(auth_path) -> None:
    assert not os.path.isfile(auth_path)

    expected_path = get_auth_file_path(auth_path)

    assert os.path.isfile(expected_path)
    assert expected_path == auth_path


def test_get_configparser_throws_if_path_is_corrupted() -> None:
    mock_config = MagicMock()
    mock_config.read.side_effect = PermissionError("Permission denied")

    with patch("configparser.ConfigParser", return_value=mock_config):
        with pytest.raises(ValueError):
            get_configparser_object(auth_path=Path())


def test_create_blank_file_throws_if_configfile_path_is_corrupted() -> None:
    mock_config = MagicMock()
    mock_config.read.side_effect = OSError("Some weird OS behavior")

    with patch("configparser.ConfigParser", return_value=mock_config):
        with pytest.raises(ValueError):
            _create_blank_auth_file(auth_path=Path())


def test_create_blank_file_throws_if_cant_touch_new_file() -> None:
    mock_config = MagicMock()
    mock_config.touch.side_effect = PermissionError("Some weird OS behavior")

    with patch("pathlib.Path.touch", return_value=mock_config):
        with pytest.raises(ValueError):
            _create_blank_auth_file(auth_path=Path())


def test_create_blank_file_throws_if_cant_create_file() -> None:
    mock_config = MagicMock()
    mock_config.touch.side_effect = PermissionError("Some weird OS behavior")

    with patch("pathlib.Path.mkdir", return_value=mock_config):
        with pytest.raises(ValueError):
            _create_blank_auth_file(auth_path=Path())


def test_create_blank_file_throws_if_cant_write_configfile() -> None:
    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.side_effect = IOError("Cant write here")
        with pytest.raises(ValueError):
            _create_blank_auth_file(auth_path=Path())

    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.return_value.write.side_effect = PermissionError("Cant write here")
        with pytest.raises(ValueError):
            _create_blank_auth_file(auth_path=Path())
