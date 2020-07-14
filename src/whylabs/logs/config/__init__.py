import yaml

from .config import WhyLogsConfig


def parse(stream) -> WhyLogsConfig:
    import yaml
    return yaml.parse(stream)


def to_yaml(config: WhyLogsConfig, stream=None):
    return yaml.dump(config, stream)
