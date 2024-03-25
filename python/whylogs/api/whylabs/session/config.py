import configparser
import getpass
import logging
import os
from configparser import ConfigParser
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

from platformdirs import user_config_dir

from whylogs.api.whylabs.session.notebook_check import is_interractive
from whylogs.api.whylabs.session.prompts import (
    prompt_api_key,
    prompt_default_dataset_id,
    prompt_org_id,
    prompt_session_type,
)
from whylogs.api.whylabs.session.session_types import ApiKeyV1, ApiKeyV2
from whylogs.api.whylabs.session.session_types import InteractiveLogger as il
from whylogs.api.whylabs.session.session_types import (
    SessionType,
    parse_api_key,
    parse_api_key_v2,
)

_DEFAULT_WHYLABS_HOST = "https://api.whylabsapp.com"
_CONFIG_APP_NAME = "whylogs"
INIT_DOCS = "https://docs.whylabs.ai/docs/whylabs-whylogs-init"


class InitException(Exception):
    pass


class EnvVariableName(Enum):
    WHYLABS_API_KEY = "WHYLABS_API_KEY"
    WHYLABS_SESSION_ID = "WHYLABS_SESSION_ID"
    WHYLABS_ORG_ID = "WHYLABS_DEFAULT_ORG_ID"
    WHYLABS_DEFAULT_DATASET_ID = "WHYLABS_DEFAULT_DATASET_ID"
    WHYLOGS_CONFIG_PATH = "WHYLOGS_CONFIG_PATH"
    WHYLABS_API_ENDPOINT = "WHYLABS_API_ENDPOINT"
    WHYLABS_REFERENCE_PROFILE_NAME = "WHYLABS_REFERENCE_PROFILE_NAME"
    WHYLABS_PRIVATE_API_ENDPOINT = "WHYLABS_PRIVATE_API_ENDPOINT"
    WHYLABS_PRIVATE_S3_ENDPOINT = "WHYLABS_PRIVATE_S3_ENDPOINT"
    HTTP_PROXY = "HTTP_PROXY"
    HTTPS_PROXY = "HTTPS_PROXY"


class ConfigVariableName(Enum):
    API_KEY = "api_key"
    ORG_ID = "org_id"
    SESSION_ID = "session_id"
    WHYLABS_API_ENDPOINT = "whylabs_api_endpoint"
    USER_GUID = "user_guid"
    DEFAULT_DATASET_ID = "default_dataset_id"
    WHYLABS_REFERENCE_PROFILE_NAME = "whylabs_reference_profile_name"
    WHYLABS_PRIVATE_API_ENDPOINT = "whylabs_private_api_endpoint"
    WHYLABS_PRIVATE_S3_ENDPOINT = "whylabs_private_s3_endpoint"
    HTTP_PROXY = "http_proxy"
    HTTPS_PROXY = "https_proxy"


@dataclass
class InitConfig:
    whylabs_api_key: Optional[str] = None
    allow_anonymous: bool = True
    allow_local: bool = False
    default_dataset_id: Optional[str] = None
    config_path: Optional[str] = None
    force_local: Optional[bool] = None


class SessionConfig:
    def __init__(self, init_config: Optional[InitConfig] = None) -> None:
        self._init_config = init_config or InitConfig()
        self.logger = logging.getLogger("config")
        self._ensure_config_exists = False if self._init_config.force_local is True else True
        self.auth_path = (
            Path(self._init_config.config_path) if self._init_config.config_path else self.get_config_file_path()
        )
        self._init_parser()

        self.tmp_api_key: Optional[str] = self._init_config.whylabs_api_key
        self.tmp_default_dataset_id: Optional[str] = self._init_config.default_dataset_id

        # Only here for CLI compatibilty. No one should actually set this.
        force_interactive = os.environ.get("WHYLABS_FORCE_INTERACTIVE", "false").lower() == "true"
        if force_interactive:
            self.reset_config()
            self.session_type = self._determine_session_type_prompt(self._init_config)
        else:
            self.session_type = self._determine_session_type(self._init_config)

    def _init_parser(self) -> None:
        try:
            self._config_parser = ConfigFile.create_parser(self.auth_path)
        except (configparser.Error, OSError, IOError, PermissionError) as e:
            self.logger.warning(
                f"Error reading config file from {self.auth_path}: {str(e)}. Operations with WhyLabs will fail."
            )

    def get_session_type(self) -> SessionType:
        return self.session_type

    def _load_value(self, env_name: EnvVariableName, config_name: ConfigVariableName) -> Optional[str]:
        """
        Load a configuration value, first checking the environment, then falling back to the config file
        """
        env_variable = os.getenv(env_name.value)
        if env_variable is not None:
            return env_variable

        return ConfigFile.get_variable_from_config_file(self._config_parser, config_name)

    def _load_or_prompt(
        self,
        env_name: EnvVariableName,
        config_name: ConfigVariableName,
        persist: bool = False,
        password: bool = True,
    ) -> Optional[str]:
        """
        Loads a configuration value like _load_value does, but it will also prompt the user for the value if
        there isn't one in either of those places, assuming we're in a notebook environment. If persist is True,
        the prompt value will also be persisted to the configuration file.
        """
        value = self._load_value(env_name=env_name, config_name=config_name)

        if value is None and is_interractive():
            if password:
                prompt_value = ConfigFile.get_variable_from_getpass(config_name)
            else:
                prompt_value = ConfigFile.get_variable_from_input(config_name)

            if persist:
                ConfigFile.set_variable_to_config_file(self._config_parser, self.auth_path, config_name, prompt_value)
            return prompt_value
        else:
            return value

    def _remove_value(self, key: ConfigVariableName) -> None:
        ConfigFile.remove_variable_from_config_file(self._config_parser, self.auth_path, key)

    def _set_value(self, key: ConfigVariableName, value: str) -> None:
        ConfigFile.set_variable_to_config_file(
            parser=self._config_parser,
            auth_path=self.auth_path,
            key=key,
            value=value,
        )

    def get_https_proxy(self) -> Optional[str]:
        return self._load_value(
            env_name=EnvVariableName.HTTPS_PROXY,
            config_name=ConfigVariableName.HTTPS_PROXY,
        )

    def set_https_proxy(self, proxy: str) -> None:
        self._set_value(ConfigVariableName.HTTPS_PROXY, proxy)

    def get_http_proxy(self) -> Optional[str]:
        return self._load_value(
            env_name=EnvVariableName.HTTP_PROXY,
            config_name=ConfigVariableName.HTTP_PROXY,
        )

    def set_http_proxy(self, proxy: str) -> None:
        self._set_value(ConfigVariableName.HTTP_PROXY, proxy)

    def get_whylabs_private_api_endpoint(self) -> Optional[str]:
        return self._load_value(
            env_name=EnvVariableName.WHYLABS_PRIVATE_API_ENDPOINT,
            config_name=ConfigVariableName.WHYLABS_PRIVATE_API_ENDPOINT,
        )

    def set_whylabs_private_api_endpoint(self, endpoint: str) -> None:
        self._set_value(ConfigVariableName.WHYLABS_PRIVATE_API_ENDPOINT, endpoint)

    def get_whylabs_private_s3_endpoint(self) -> Optional[str]:
        return self._load_value(
            env_name=EnvVariableName.WHYLABS_PRIVATE_S3_ENDPOINT,
            config_name=ConfigVariableName.WHYLABS_PRIVATE_S3_ENDPOINT,
        )

    def set_whylabs_private_s3_endpoint(self, endpoint: str) -> None:
        self._set_value(ConfigVariableName.WHYLABS_PRIVATE_S3_ENDPOINT, endpoint)

    def get_whylabs_refernce_profile_name(self) -> Optional[str]:
        return self._load_value(
            env_name=EnvVariableName.WHYLABS_REFERENCE_PROFILE_NAME,
            config_name=ConfigVariableName.WHYLABS_REFERENCE_PROFILE_NAME,
        )

    def set_whylabs_refernce_profile_name(self, name: str) -> None:
        self._set_value(ConfigVariableName.WHYLABS_REFERENCE_PROFILE_NAME, name)

    def get_whylabs_endpoint(self) -> str:
        return (
            self._load_value(
                env_name=EnvVariableName.WHYLABS_API_ENDPOINT,
                config_name=ConfigVariableName.WHYLABS_API_ENDPOINT,
            )
            or _DEFAULT_WHYLABS_HOST
        )

    def set_whylabs_endpoint(self, endpoint: str) -> None:
        self._set_value(ConfigVariableName.WHYLABS_API_ENDPOINT, endpoint)

    def get_config_file_path(self) -> Path:
        config_dir_path = os.getenv(EnvVariableName.WHYLOGS_CONFIG_PATH.value)
        if config_dir_path is not None:
            Path(config_dir_path).mkdir(parents=True, exist_ok=True)
        else:
            config_dir_path = user_config_dir(_CONFIG_APP_NAME, ensure_exists=self._ensure_config_exists)

        config_file_path = os.path.join(config_dir_path, "config.ini")
        path = Path(config_file_path)
        if self._ensure_config_exists:
            path.touch(exist_ok=True)
        return path

    def get_default_dataset_id(self) -> Optional[str]:
        return self.tmp_default_dataset_id or self._load_value(
            env_name=EnvVariableName.WHYLABS_DEFAULT_DATASET_ID,
            config_name=ConfigVariableName.DEFAULT_DATASET_ID,
        )

    def require_default_dataset_id(self) -> str:
        return self._require("default dataset id", self.get_default_dataset_id())

    def set_default_dataset_id(self, dataset_id: str) -> None:
        self._set_value(ConfigVariableName.DEFAULT_DATASET_ID, dataset_id)

    def get_org_id(self) -> Optional[str]:
        # Make the v2 api key's org id take precedence over an org id previously stored along with a v1 key
        api_key = self.get_api_key()
        try:
            if api_key is not None:
                return parse_api_key_v2(api_key).org_id
        except Exception:
            pass

        org_id = self._load_value(
            env_name=EnvVariableName.WHYLABS_ORG_ID,
            config_name=ConfigVariableName.ORG_ID,
        )

        if org_id is not None:
            return org_id

        return org_id

    def require_org_id(self) -> str:
        return self._require("org id", self.get_org_id())

    def _require(self, name: str, value: Optional[str]) -> str:
        if value is None:
            session_type = self.get_session_type()
            raise ValueError(
                f"Can't determine {name}. Current session type is {session_type.value}. "
                f"See {INIT_DOCS} for instructions on using why.init()."
            )

        return value

    def set_org_id(self, org_id: str) -> None:
        self._set_value(ConfigVariableName.ORG_ID, org_id)

    def get_api_key(self) -> Optional[str]:
        return self.tmp_api_key or self._load_value(
            env_name=EnvVariableName.WHYLABS_API_KEY,
            config_name=ConfigVariableName.API_KEY,
        )

    def get_env_api_key(self) -> Optional[str]:
        return os.getenv(EnvVariableName.WHYLABS_API_KEY.value)

    def require_api_key(self) -> str:
        return self._require("api key", self.get_api_key())

    def set_api_key(self, api_key: str) -> None:
        self._set_value(ConfigVariableName.API_KEY, api_key)

    def get_user_guid(self) -> Optional[str]:
        return ConfigFile.get_variable_from_config_file(self._config_parser, ConfigVariableName.USER_GUID)

    def set_user_guid(self, user_guid: str) -> None:
        self._set_value(ConfigVariableName.USER_GUID, user_guid)

    def get_session_id(self) -> Optional[str]:
        return self._load_value(
            env_name=EnvVariableName.WHYLABS_SESSION_ID,
            config_name=ConfigVariableName.SESSION_ID,
        )

    def set_session_id(self, sessionId: str) -> None:
        self._set_value(ConfigVariableName.SESSION_ID, sessionId)

    def remove_session_id(self) -> None:
        self._remove_value(ConfigVariableName.SESSION_ID)

    def reset_config(self) -> None:
        ConfigFile.remove_all_variables_from_config_file(parser=self._config_parser, auth_path=self.auth_path)
        self._init_parser()

    def notify_session_type(self) -> None:
        config_path = self.get_config_file_path()
        il.message(f"Initializing session with config {config_path}", ignore_suppress=True)
        il.message(ignore_suppress=True)
        if self.session_type == SessionType.WHYLABS:
            self._notify_type_whylabs(self.require_api_key())
        elif self.session_type == SessionType.LOCAL:
            self._notify_type_local()
        elif self.session_type == SessionType.WHYLABS_ANONYMOUS:
            self._notify_type_anon()

    def _notify_type_whylabs(self, api_key: str) -> None:
        default_dataset_id = self.get_default_dataset_id()
        parsed_api_key = parse_api_key(api_key)
        if isinstance(parsed_api_key, ApiKeyV2):
            org_id = parsed_api_key.org_id
        else:
            org_id = self.get_org_id() or "not set"  # Shouldn't be possible to be None at this point

        il.success(f"Using session type: {SessionType.WHYLABS.name}", ignore_suppress=True)
        il.option(f"org id: {org_id}", ignore_suppress=True)
        il.option(f"api key: {parsed_api_key.api_key_id}", ignore_suppress=True)
        if default_dataset_id:
            il.option(f"default dataset: {default_dataset_id}")

        il.message()
        il.message(
            f"In production, you should pass the api key as an environment variable {EnvVariableName.WHYLABS_API_KEY.value}, "
            f"the org id as {EnvVariableName.WHYLABS_ORG_ID.value}, and the default dataset id as "
            f"{EnvVariableName.WHYLABS_DEFAULT_DATASET_ID.value}."
        )

    def _notify_type_anon(self) -> None:
        anonymous_session_id = self.get_session_id()
        il.success(
            f"Using session type: {SessionType.WHYLABS_ANONYMOUS.name}",
            ignore_suppress=True,
        )
        id_text = "<will be generated before upload>" if not anonymous_session_id else anonymous_session_id
        il.option(f"session id: {id_text}", ignore_suppress=True)

    def _notify_type_local(self) -> None:
        il.success(
            f"Using session type: {SessionType.LOCAL.name}. "
            "Profiles won't be uploaded or written anywhere automatically.",
            ignore_suppress=True,
        )

    def _determine_session_type_prompt(self, init_config: InitConfig) -> SessionType:
        session_type = prompt_session_type(init_config.allow_anonymous, init_config.allow_local)
        if session_type == SessionType.WHYLABS:
            api_key = prompt_api_key()

            self.set_api_key(api_key.full_key)
            if isinstance(api_key, ApiKeyV1):
                # V1 doesn't include the org id so we have to prompt for it separately
                org_id = prompt_org_id()
                self.set_org_id(org_id)

            # Save the default dataset id if they supplied one
            default_dataset_id = prompt_default_dataset_id()
            if default_dataset_id:
                self.set_default_dataset_id(default_dataset_id)

        return session_type

    def _determine_session_type(self, init_config: InitConfig) -> SessionType:
        if init_config.force_local:
            return SessionType.LOCAL

        # If the user supplied a whylabs api key then use whylabs
        if init_config.whylabs_api_key is not None:
            return SessionType.WHYLABS

        # If there is an api key saved in the config file or in the env then use that for whylabs
        config_api_key = self.get_api_key()
        if config_api_key is not None:
            return SessionType.WHYLABS

        # If there is a session id saved in the config file then use that for an anonymous session
        anonymous_session_id = self.get_session_id()
        if anonymous_session_id is not None and init_config.allow_anonymous:
            return SessionType.WHYLABS_ANONYMOUS

        # If we're in an interactive environment then prompt the user to pick an authentication method
        if is_interractive():
            return self._determine_session_type_prompt(init_config)

        # If we're not interactive then pick between anonymous and local based on the init config
        if init_config.allow_anonymous:
            return SessionType.WHYLABS_ANONYMOUS

        if init_config.allow_local:
            return SessionType.LOCAL

        raise InitException(
            "Don't know how to initialize authentication because allow_anonymous=False, allow_local=False, "
            "and there is no WhyLabs api key in the environment, config file, or why.init() call, and this isn't an "
            f"interactive environment. See {INIT_DOCS} for instructions on using why.init()."
        )


_CONFIG_WHYLABS_SECTION = "whylabs"


class ConfigFile:
    @staticmethod
    def create_parser(config_path: Path) -> ConfigParser:
        config_parser = ConfigParser()
        config_parser.read(config_path)
        if not config_parser.has_section(_CONFIG_WHYLABS_SECTION):
            config_parser[_CONFIG_WHYLABS_SECTION] = {}
        return config_parser

    @staticmethod
    def get_variable_from_input(variable_name: ConfigVariableName) -> str:
        variable = input(f"What is your {variable_name.value}? ")
        if not variable:
            raise ValueError(f"Interrupted before input was entered for {variable_name.value}")
        return variable

    @staticmethod
    def get_variable_from_getpass(variable_name: ConfigVariableName) -> str:
        variable = getpass.getpass(f"What is your {variable_name.value}? ")
        if not variable:
            raise ValueError(f"Interrupted before input was entered for {variable_name.value}")
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

    @staticmethod
    def remove_variable_from_config_file(parser: ConfigParser, auth_path: Path, key: ConfigVariableName) -> None:
        parser.remove_option(_CONFIG_WHYLABS_SECTION, key.value)
        with open(auth_path, "w") as configfile:
            parser.write(configfile)

    @staticmethod
    def remove_all_variables_from_config_file(parser: ConfigParser, auth_path: Path) -> None:
        parser.remove_section(_CONFIG_WHYLABS_SECTION)
        with open(auth_path, "w") as configfile:
            parser.write(configfile)
