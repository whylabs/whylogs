import configparser
from pathlib import Path
from typing import Optional

import keyring

from .auth_file import get_configparser_object


class Variables:
    @staticmethod
    def get_variable_from_input(variable_name: str) -> str:
        variable = None
        while not variable:
            variable = input(f"What is your {variable_name}? ")
        return variable

    @staticmethod
    def get_variable_from_config_file(config: configparser.ConfigParser, key: str) -> Optional[str]:
        value = config.get(section="whylabs", option=key, fallback=None)
        return value

    @staticmethod
    def get_password_from_keyring() -> str:
        api_key = keyring.get_password("whylabs", "api_key")
        if api_key is None:
            raise ValueError(
                "You must set your api_key either with keyring or the WHYLABS_API_KEY environment variable"
            )
        keyring.set_password("whylabs", "api_key", api_key)
        return api_key

    @staticmethod
    def set_variable_to_config_file(key: str, value: str, auth_path: Path) -> None:
        config = get_configparser_object(auth_path=auth_path)
        config.set("whylabs", key, value)
        with open(auth_path, "w") as configfile:
            config.write(configfile)
