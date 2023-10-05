import os

_FALSY_STRINGS = ["0", "false", "", "no", "off", "disabled", "n"]


def read_bool_env_var(env_var_name, default_value=False):
    env_var_value = os.environ.get(env_var_name)

    if env_var_value is None:
        return default_value

    return env_var_value.lower() not in _FALSY_STRINGS
