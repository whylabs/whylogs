import configparser

import pytest

from whylogs.api.whylabs.variables import Variables


@pytest.fixture
def create_config_file(auth_path):
    conf = configparser.ConfigParser()
    conf.read(auth_path)
    conf["whylabs"] = {}
    conf.set("whylabs", "my_key", "result")
    with open(auth_path, "w") as configfile:
        conf.write(configfile)
    yield


def test_get_variable_from_input(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "my_answer")

    variable = Variables.get_variable_from_input("my_key")
    assert variable == "my_answer"


def test_get_variable_from_input_stops_with_no_answer(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "")

    with pytest.raises(ValueError):
        Variables.get_variable_from_input("my_key")


def test_get_variable_from_config_file(auth_path, create_config_file):
    result = Variables.get_variable_from_config_file(auth_path=auth_path, key="my_key")

    assert result == "result"


def test_set_variable_on_config_file(auth_path, create_config_file):
    Variables.set_variable_to_config_file(auth_path=auth_path, key="my_key", value="other_value")

    config = configparser.ConfigParser()
    config.read(auth_path)
    assert config.get("whylabs", "my_key") == "other_value"
