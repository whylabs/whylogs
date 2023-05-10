import getpass
import logging
import os
from pathlib import Path
from typing import Optional
from enum import Enum
from platformdirs import user_config_dir
from configparser import ConfigParser
import configparser

from whylogs.api.whylabs.notebook_check import is_notebook

_DEFAULT_WHYLABS_HOST = "https://api.whylabsapp.com"
_CONFIG_APP_NAME = "whylogs"


class EnvVariableName(Enum):
    WHYLABS_API_KEY = "WHYLABS_API_KEY"
    WHYLABS_SESSION_ID = "WHYLABS_SESSION_ID"
    ORG_ID = "ORG_ID"
    WHYLOGS_CONFIG_PATH = "WHYLOGS_CONFIG_PATH"
    WHYLABS_API_ENDPOINT = "WHYLABS_API_ENDPOINT"


class ConfigVariableName(Enum):
    API_KEY = "api_key"
    ORG_ID = "org_id"
    SESSION_ID = "session_id"
    WHYLABS_API_ENDPOINT = "whylabs_api_endpoint"


class SessionConfig:
    def __init__(self) -> None:
        self.logger = logging.getLogger("config")
        self.auth_path = self.get_config_file_path()
        try:
            self._config_parser = ConfigFile.create_parser(self.auth_path)
        except (configparser.Error, OSError, IOError, PermissionError) as e:
            self.logger.warning(
                f"Error reading config file from {self.auth_path}: {str(e)}. Operations with WhyLabs will fail."
            )

    def _load_value(self, env_name: EnvVariableName, config_name: ConfigVariableName) -> Optional[str]:
        """
        Load a configuration value, first checking the environment, then falling back to the config file
        """
        env_variable = os.getenv(env_name.value)
        if env_variable is not None:
            return env_variable

        return ConfigFile.get_variable_from_config_file(self._config_parser, config_name)

    def _load_or_prompt(
        self, env_name: EnvVariableName, config_name: ConfigVariableName, persist: bool = False, password: bool = True
    ) -> Optional[str]:
        value = self._load_value(env_name=env_name, config_name=config_name)

        if value is None and is_notebook():
            if password:
                prompt_value = ConfigFile.get_variable_from_getpass(config_name)
            else:
                prompt_value = ConfigFile.get_variable_from_input(config_name)

            if persist:
                ConfigFile.set_variable_to_config_file(self._config_parser, self.auth_path, config_name, prompt_value)
            return prompt_value
        else:
            return value

    def get_whylabs_endpoint(self) -> str:
        return (
            self._load_value(
                env_name=EnvVariableName.WHYLABS_API_ENDPOINT, config_name=ConfigVariableName.WHYLABS_API_ENDPOINT
            )
            or _DEFAULT_WHYLABS_HOST
        )

    def set_whylabs_endpoint(self, endpoint: str) -> None:
        ConfigFile.set_variable_to_config_file(
            parser=self._config_parser,
            auth_path=self.auth_path,
            key=ConfigVariableName.WHYLABS_API_ENDPOINT,
            value=endpoint,
        )

    # TODO maybe make this Optional[Path] and let everything fail with warnings elsewhere
    def get_config_file_path(self) -> Path:
        # TODO this can probably fail depending on the environment
        config_dir_path = os.getenv(EnvVariableName.WHYLOGS_CONFIG_PATH.value) or user_config_dir(
            _CONFIG_APP_NAME, ensure_exists=True
        )
        config_file_path = os.path.join(config_dir_path, "config.ini")
        path = Path(config_file_path)
        path.touch(exist_ok=True)
        return path

    def get_org_id(self) -> Optional[str]:
        return self._load_value(env_name=EnvVariableName.ORG_ID, config_name=ConfigVariableName.ORG_ID)

    def get_or_prompt_org_id(self, persist: bool = False) -> Optional[str]:
        return self._load_or_prompt(
            env_name=EnvVariableName.ORG_ID, config_name=ConfigVariableName.ORG_ID, password=False, persist=persist
        )

    def get_api_key(self) -> Optional[str]:
        return self._load_value(env_name=EnvVariableName.WHYLABS_API_KEY, config_name=ConfigVariableName.API_KEY)

    def get_or_prompt_api_key(self, persist: bool = False) -> Optional[str]:
        return self._load_or_prompt(
            env_name=EnvVariableName.WHYLABS_API_KEY, config_name=ConfigVariableName.API_KEY, persist=persist
        )

    def get_session_id(self) -> Optional[str]:
        return self._load_value(env_name=EnvVariableName.WHYLABS_SESSION_ID, config_name=ConfigVariableName.SESSION_ID)

    def set_session_id(self, sessionId: str) -> None:
        ConfigFile.set_variable_to_config_file(
            parser=self._config_parser, auth_path=self.auth_path, key=ConfigVariableName.SESSION_ID, value=sessionId
        )


_CONFIG_WHYLABS_SECTION = "whylabs"


class ConfigFile:
    @staticmethod
    def create_parser(config_path: Path) -> ConfigParser:
        config_parser = ConfigParser()
        config_parser.read(config_path)
        if config_parser[_CONFIG_WHYLABS_SECTION] is None:
            config_parser[_CONFIG_WHYLABS_SECTION] = {}
        return config_parser

    @staticmethod
    def get_variable_from_input(variable_name: ConfigVariableName) -> str:
        variable = input(f"What is your {variable_name}? ")
        if not variable:
            raise ValueError(f"You must define a {variable_name}")
        return variable

    @staticmethod
    def get_variable_from_getpass(variable_name: ConfigVariableName) -> str:
        variable = getpass.getpass(f"What is your {variable_name.value}? ")
        if not variable:
            raise ValueError(f"You must define a {variable_name.value}")
        return variable

    @staticmethod
    def get_variable_from_config_file(parser: ConfigParser, key: ConfigVariableName) -> Optional[str]:
        value = parser.get(_CONFIG_WHYLABS_SECTION, key.value, fallback=None)
        return value

    @staticmethod
    def set_variable_to_config_file(parser: ConfigParser, auth_path: Path, key: ConfigVariableName, value: str) -> None:
        parser.set(_CONFIG_WHYLABS_SECTION, key.value, value)
        with open(auth_path, "w") as configfile:
            parser.write(configfile)
