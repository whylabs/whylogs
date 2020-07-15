import yaml

from .config import WhyLogsConfig, ConfigDateTime


def parse(stream) -> WhyLogsConfig:
    import yaml
    return yaml.parse(stream)


def to_yaml(cfg: WhyLogsConfig, stream=None):
    return yaml.dump(cfg, stream)
