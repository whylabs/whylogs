"""
"""
from copy import deepcopy
from logging import getLogger


class HandlerConfig:
    """
    Simple config class
    """
    OBJECT_TYPES = ('dataset_summary', 'dataset_profile', )
    FORMATS = ('json', 'flat', 'protobuf')
    DESTINATIONS = ('s3', 'disk', 'stdout')

    def __init__(self, object_type, fmt, dest):
        assert object_type in self.OBJECT_TYPES
        assert fmt in self.FORMATS
        assert dest in self.DESTINATIONS
        self.object_type = object_type
        self.fmt = fmt
        self.dest = dest

    def __repr__(self):
        import json
        d = self.to_dict()
        try:
            msg = json.dumps(d, indent=2, sort_keys=True)
        except TypeError:
            msg = str(d)
        msg = '<HandlerConfig>\n' + msg
        return msg

    def to_dict(self):
        return {
            'object_type': self.object_type,
            'fmt': self.fmt,
            'dest': self.dest,
        }


class EnvConfig:
    """
    Abstract base class with can read _config from the environment and from
    YAML buffers
    """
    _default_env_prefix = None
    _config_defaults = None
    _derived_config = tuple()
    _parsers = None  # dict containing functions to parse/process fields

    def __init__(self, config=None, env_prefix=None):
        if env_prefix is None:
            env_prefix = self._default_env_prefix
        self.env_prefix = env_prefix
        if self._config_defaults is None:
            raise NotImplementedError("Class _config_defaults not implemented")
        self._config = deepcopy(self._config_defaults)
        if config is not None:
            self._config.update(config)
        self._writeable_keys = set(self._config.keys())
        self._readable_keys = set(list(self._config.keys())
                                  + list(self._derived_config))
        self._read_only_keys = self._readable_keys - self._writeable_keys
        for k in self._readable_keys:
            if k.startswith('_'):
                raise ValueError("Config names cannot start with '_'")

    def __getitem__(self, item):
        if item in self._read_only_keys:
            raise NotImplemented(f"Derived-only item {item} not implemented")
        return self._config[item]

    def __setitem__(self, key, value):
        if key in self._read_only_keys:
            raise KeyError(f"Cannot write read-only key {key}")
        self.set_opts(**{key: value})

    def __repr__(self):
        import json
        d = self.to_dict()
        try:
            msg = json.dumps(d, indent=2, sort_keys=True)
        except TypeError:
            msg = str(d)
        msg = '<EnvConfig>\n' + msg
        return msg

    def to_dict(self):
        """
        Return a dictionary of the config
        """
        output = {}
        for k in self._writeable_keys:
            v = self[k]
            if isinstance(v, EnvConfig) or hasattr(v, 'to_dict'):
                try:
                    v = v.to_dict()
                except Exception:
                    pass
            output[k] = v
        return output

    def list_keys(self):
        """
        List keys in the config
        """
        return [k for k in self._writeable_keys]

    def list_env_keys(self):
        """
        List all environmental variables that this config will parse
        """
        keys = []
        prefix = self.env_prefix
        if prefix is None:
            prefix = ''
        prefix = prefix.upper()
        for k in self._writeable_keys:
            keys.append(prefix + k.upper())
        return sorted(keys)

    def list_readable_keys(self):
        """
        List keys we can read
        """
        return [k for k in self._readable_keys]

    def set_opts(self, **kwargs):
        """
        Set options
        """
        import yaml
        parsed_opts = {}
        for k in kwargs.keys():
            if k not in self._readable_keys:
                raise ValueError(f"Unrecognized option: {k}")
            elif k in self._read_only_keys:
                raise ValueError(f"Cannot set write-only key: {k}")

        for k, v in kwargs.items():
            if isinstance(v, str):
                v = yaml.safe_load(v)
            parsed_opts[k] = v

        # Apply any user-defined parser functions
        parsers = self._parsers
        if parsers is None:
            parsers = {}
        for k, parser in parsers.items():
            if k in parsed_opts:
                parsed_opts[k] = parser(parsed_opts[k])

        getLogger(__name__).debug(f'Setting _config opts: {parsed_opts}')
        self._config.update(parsed_opts)

    def parse_env(self):
        """
        Parse environmental variables
        """
        env = self._get_env()
        self.set_opts(**env)

    def add_yaml(self, buffer):
        """
        Parse a YAML buffer
        """
        import yaml
        data = yaml.safe_load(buffer)
        assert isinstance(data, dict)
        self.set_opts(**data)

    def _get_env(self):
        import os
        env = {}
        env_prefix = self.env_prefix
        if env_prefix is None:
            env_prefix = ''
        env_prefix = env_prefix.upper()

        for k in self._writeable_keys:
            env_key = env_prefix + k.upper()
            if env_key in os.environ:
                env[k] = os.environ[env_key]

        return env

    def validate(self):
        for k in self._readable_keys:
            assert k.lower() == k
        # Try to access all attributes
        cfg_dict = self.to_dict()


class ConfigParams(EnvConfig):
    """
    Config for WhyLogs logging
    """
    _default_env_prefix = "WHYLOGS_"
    _config_defaults = {
        'project': None,
        'pipeline': None,
        'user': None,
        'team': None,
        # Output config
        'level': 'debug',
        # Output destination config
        'bucket': None,
        'local_output_folder': './whylogs',
        'cloud_output_folder': None,
        # Boolean output flags
        'output_to_disk': True,
        'output_to_cloud': False,
        'output_to_stdout': False,  # not yet implemented
        'output_protobuf': True,
        'output_json_summary': True,
        'output_flat_summary': True,
        'disable_output': False,
    }
    _parsers = None  # dict containing functions to parse/process fields
    _derived_config = {
        'output_summary',
    }

    def __getitem__(self, item):
        if item not in self._derived_config:
            return super().__getitem__(item)

        elif item == 'output_summary':
            return self['output_json_summary'] or self['output_flat_summary']


def _get_handler_configs(cfg: ConfigParams):
    destinations = []
    if cfg['output_to_cloud']:
        destinations.append('s3')
    if cfg['output_to_disk']:
        destinations.append('disk')
    if cfg['output_to_stdout']:
        destinations.append('stdout')

    handler_configs = []
    if cfg['output_protobuf']:
        for dest in destinations:
            handler_configs.append(
                HandlerConfig('dataset_profile', 'protobuf', dest))

    for dest in destinations:
        if cfg['output_flat_summary']:
            handler_configs.append(
                HandlerConfig('dataset_summary', 'flat', dest)
            )
        if cfg['output_json_summary']:
            handler_configs.append(
                HandlerConfig('dataset_summary', 'json', dest)
            )

    return handler_configs, destinations


def _get_output_steps(handler_configs: list):
    steps = {}
    for h in handler_configs:
        if h.object_type not in steps:
            steps[h.object_type] = {}
        a = steps[h.object_type]
        if h.fmt not in a:
            a[h.fmt] = []
        a[h.fmt].append(h.dest)
    return steps


def load_config(**kwargs):
    """
    Load logging configuration, from disk and from the environment.

    Config is loaded in steps as follows, with later steps taking precedence:

    1. ~/.whylogs.yaml
    2. ./whylogs.yaml   (within current directory)
    3. Environment
    """
    import os
    logger = getLogger(__name__)
    user_file = os.path.join(os.path.expanduser('~'), '.whylogs.yaml')
    files = [user_file, 'whylogs.yaml', ]
    cfg = ConfigParams()
    for fname in files:
        logger.debug(f'Attempting to load config file: {fname}')
        try:
            cfg.add_yaml(open(fname, 'rt'))
        except FileNotFoundError:
            pass
    logger.debug('Parsing env')
    cfg.parse_env()
    cfg.set_opts(**kwargs)
    return cfg

