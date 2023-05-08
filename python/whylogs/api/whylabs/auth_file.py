import configparser
from pathlib import Path


def _create_blank_auth_file(auth_path: Path) -> Path:
    try:
        # creates directory and file
        auth_path.parent.mkdir(parents=True, exist_ok=True)
        auth_path.touch(exist_ok=True)

        # creates empty config file
        config = configparser.ConfigParser()
        config.read(auth_path)
        config["whylabs"] = {}

        # writes config file to disk
        with open(auth_path, "w") as configfile:
            config.write(configfile)
        return auth_path

    except (configparser.Error, OSError, IOError, PermissionError) as e:
        raise ValueError(f"Error creating auth file at {auth_path}: {str(e)}") from e


def get_auth_file_path(auth_path: Path) -> Path:
    try:
        auth_file_path = auth_path.resolve(strict=True)
    except FileNotFoundError:
        auth_file_path = _create_blank_auth_file(auth_path=auth_path)
    return auth_file_path


def get_configparser_object(auth_path: Path) -> configparser.ConfigParser:
    try:
        config = configparser.ConfigParser()
        config.read(auth_path)
        return config
    except (configparser.Error, OSError, IOError, PermissionError) as e:
        raise ValueError(f"Error reading config file from {auth_path}: {str(e)}") from e
