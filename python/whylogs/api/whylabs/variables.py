import getpass
from pathlib import Path
from typing import Optional

from .auth_file import get_configparser_object


class Variables:
    @staticmethod
    def get_variable_from_input(variable_name: str) -> str:
        variable = input(f"What is your {variable_name}? ")
        if not variable:
            raise ValueError(f"You must define a {variable_name}")
        return variable

    @staticmethod
    def get_variable_from_getpass(variable_name: str) -> str:
        variable = getpass.getpass(f"What is your {variable_name}? ")
        if not variable:
            raise ValueError(f"You must define a {variable_name}")
        return variable

    @staticmethod
    def get_variable_from_config_file(auth_path: Path, key: str) -> Optional[str]:
        config = get_configparser_object(auth_path=auth_path)
        value = config.get("whylabs", key, fallback=None)
        return value

    @staticmethod
    def set_variable_to_config_file(key: str, value: str, auth_path: Path) -> None:
        config = get_configparser_object(auth_path=auth_path)
        config.set("whylabs", key, value)
        with open(auth_path, "w") as configfile:
            config.write(configfile)