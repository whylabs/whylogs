import configparser, os


def get_env(environment, variable) -> str:
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    return cfg.get(environment, variable, vars=os.environ)
