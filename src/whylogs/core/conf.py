import os

from typing import Optional


# noinspection PyPep8Naming
class WhyConf(object):
    def __init__(self, loadEnv=True):
        self._config = {}
        if loadEnv:
            self._loadEnv()

    def setCore(self, key: str, value: str):
        """

        Returns:
            WhyConf: the current configuration object
        """
        self._config[f'whylogs.core.{key}'] = value
        return self

    def addWriter(self, writer_type: str):
        writers = self._config.get(WHYLOGS_WRITERS.key) or ''
        writers_set = set(writers.split(','))
        writers_set.add(writer_type)
        writers_set.add(sorted(list(writers_set)))

        self._config[WHYLOGS_WRITERS.key] = ','.join(writers_set)

    def setWhyLabsOrgId(self, org_id: str):
        self._config['whylabs.orgId'] = org_id
        return self

    def setWhyLabsApiKey(self, org_id: str, api_key: str):
        self._config[WHYLABS_ORG_ID.key] = org_id
        self._config[WHYLABS_API_KEY.key] = api_key
        return self

    def set(self, key: str, value: str):
        self._config[key] = value
        return self

    def remove(self, key: str):
        self._config.pop(key)
        return self

    def get(self, key: str, default: Optional[str] = None):
        return self._config.get(key, default)

    def getWriters(self):
        return list(self.get(WHYLOGS_WRITERS.key).split(','))

    def getWriterConf(self, writer_type: str):
        prefix = f'whylogs.writer.{writer_type}.'
        k: str
        v: str
        res = {}
        for k, v in self._config.items():
            if k.startswith(prefix):
                res[v.removeprefix(prefix)] = res

        return res

    def _loadEnv(self):
        for k, v in os.environ.items():
            normalized_key = k.lower().replace("_", ".")
            if normalized_key.startswith("whylogs."):
                self.set(normalized_key, v)
            if normalized_key.startswith("whylabs."):
                self.set(normalized_key, v)
        return self

    def copy(self):
        """
        Return a copy of the configuration. Useful to create derived copies with slight modifications
        An example is a logger might have a different configuration from the top level one

        Returns:
            WhyConf: a copy of the current object
        """
        copy = WhyConf(loadEnv=False)
        copy._config = dict(self._config)
        return copy


class WhyConfGetter(object):
    def __init__(self,
                 key: str,
                 doc: str,
                 default: Optional[str] = None):
        """

        Args:
            key (str): the configuration key
            doc (str): the documentation for the value
            default (Optional[str]) the default value for the configuration
        """
        self._key = key
        self._default = default
        self._doc = doc

    @property
    def key(self):
        return self._key

    def get(self, conf: WhyConf):
        return conf.get(self._key, default=self._default)


# List of Conf
# whylogs.core.session.id
# whylogs.core.writers
# whylogs.writer.local.root
# whylogs.writer.local.pathTemplate
# whylogs.writer.local.fileTempalte
# whylogs.writer.s3.bucket
# whylogs.writer.s3.root
# whylogs.writer.s3.pathTemplate
# whylogs.writer.s3.fileTemplate
# whylogs.writer.whylabs


WHYLOGS_WRITERS = WhyConfGetter("whylogs.writers", "List of writers that are enabled in the environment", "local")
WHYLOGS_ROTATION = WhyConfGetter("whylogs.rotation", "Frequency of log rotation. Default is disabled")
WHYLOGS_METRICS_INCLUDE = WhyConfGetter("whylogs.metrics.include",
                                        "List of metrics to include. Default is to enable standard metrics", "standard")
WHYLOGS_METRICS_EXCLUDE = WhyConfGetter("whylogs.metrics.exclude", "List of metrics to exclude. Default is empty")
WHYLOGS_METRICS_TOP_K_ENABLED = WhyConfGetter("whylogs.metrics.topk.include",
                                              "List of features/columns that have top-k enabled. Default is to enable for all features/columns",
                                              "all")
WHYLOGS_METRICS_TOP_K_DISABLED = WhyConfGetter("whylogs.metrics.topk.exclude",
                                               "List of features/columns that have top-k disabled. Default is empty",
                                               "")
WHYLOGS_FIELDS_INCLUDE = WhyConfGetter("whylogs.fields.include", "List of fields to include", "*")
WHYLOGS_FIELDS_EXCLUDE = WhyConfGetter("whylogs.fields.include", "List of fields to exclude", "*")
WHYLABS_ORG_ID = WhyConfGetter("whylabs.orgid", "local")
WHYLABS_API_KEY = WhyConfGetter("whylabs.apikey", "local")
