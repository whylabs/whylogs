import configparser
from pathlib import Path


def _create_blank_auth_file(auth_path: Path) -> Path:
    auth_path.parent.mkdir(parents=True, exist_ok=True)
    auth_path.touch(exist_ok=False)
    config = configparser.ConfigParser()
    config.read(auth_path)
    config["whylabs"] = {}
    with open(auth_path, "w") as configfile:
        config.write(configfile)
    return auth_path


def get_auth_file_path(auth_path: Path) -> Path:
    try:
        auth_file_path = auth_path.resolve(strict=True)
    except FileNotFoundError:
        auth_file_path = _create_blank_auth_file(auth_path=auth_path)
    return auth_file_path


def get_configparser_object(auth_path: Path) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(auth_path)
    return config
