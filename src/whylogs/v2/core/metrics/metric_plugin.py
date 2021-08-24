import logging as test_logging
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from json import dumps
from typing import Type

from google.protobuf import json_format

from whylogs.proto import PluginMessage, PluginType

TEST_LOGGER = test_logging.getLogger(__name__)


@dataclass
class MetricPlugin(ABC):
    """
    Abstract container class for column metrics plugins (experimental).

    """

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def target_column_name(self):
        pass

    @classmethod
    def get_subclasses(cls):
        for subclass in cls.__subclasses__():
            yield from subclass.get_subclasses()
            yield subclass

    @classmethod
    def get_name_from_type(cls, plugin_type: Type):
        module_name = plugin_type.__module__
        type_name = plugin_type.__name__ if module_name == "__main__" else module_name + "." + plugin_type.__name__
        return type_name

    @classmethod
    def name_type_match_predicate(cls, target_class_name: str, plugin_type: Type, enforce_strict_name_matching: bool) -> bool:
        if enforce_strict_name_matching:
            return target_class_name == MetricPlugin.get_name_from_type(plugin_type)
        else:
            return MetricPlugin.class_name_suffix_match_predicate(target_class_name, plugin_type)

    @classmethod
    def class_name_suffix_match_predicate(cls, target_class_name: str, plugin_type: Type) -> bool:
        if not target_class_name:
            TEST_LOGGER.warning("target_class_name is empty!")
            return False
        return target_class_name.endswith(plugin_type.__name__) and target_class_name.split(".")[-1] == plugin_type.__name__.split(".")[-1]

    @classmethod
    def from_protobuf(cls, message: PluginMessage, enforce_strict_name_matching: bool) -> "MetricPlugin":
        python_plugin = next(filter(lambda type: type.language == PluginType.Language.PYTHON, message.plugin_types), None)
        if python_plugin is None:
            raise TypeError("The given plugin does not contain a definition for Python, the plugin types are ({})".format(message.plugin_types))
        target_class_name = python_plugin.plugin_class_name

        plugin_class = next(
            filter(
                lambda plugin_class: MetricPlugin.name_type_match_predicate(target_class_name, plugin_class, enforce_strict_name_matching),
                MetricPlugin.get_subclasses(),
            ),
            None,
        )
        if plugin_class is None:
            raise TypeError(
                "There is no defined subclass of MetricPlugin matching serialized name {}, the plugin types are: {}".format(
                    target_class_name, [plugin for plugin in MetricPlugin.get_subclasses()]
                )
            )
        custom_serialization = message.WhichOneof("item")
        if custom_serialization != "struct":
            raise TypeError(
                "The given plugin message for {} does not contain a struct, if using serialized_bytes override from_protobuf() to avoid this error.".format(
                    target_class_name
                )
            )
        plugin_field = json_format.MessageToDict(message.struct)
        metric_plugin = plugin_class(**plugin_field)
        metric_plugin.name = message.name
        return metric_plugin

    def to_protobuf(
        self,
    ) -> PluginMessage:
        # Authors can override, in base implementation here we support python using full module.qualname
        # Note: there is no version checking here with respect to changes in your plugin.
        types = [
            PluginType(
                plugin_class_name=MetricPlugin.get_name_from_type(self.__class__),
                language=PluginType.Language.PYTHON,
            )
        ]
        plugin_message = PluginMessage(
            name=self.name,
            plugin_types=types,
        )
        # The protobuf struct will serialize the field names as well as the values, so plugin
        # authors might get better efficiency if you override and serialize to bytes instead.
        plugin_fields = asdict(self)
        # we don't want to duplicate the name field in the serialized fields as
        # we already have a 'name' property on PluginMessage
        plugin_fields.pop("name", None)
        plugin_message.struct.update(plugin_fields)
        return plugin_message

    @abstractmethod
    def track(self, data):
        pass

    @abstractmethod
    def merge(self, other: "MetricPlugin"):
        pass

    def to_summary(self):
        return self.serialize()

    @staticmethod
    def deserialize(data: bytes, enforce_strict_name_matching: bool = False) -> "MetricPlugin":
        message = PluginMessage.FromString(data)
        return MetricPlugin.from_protobuf(message, enforce_strict_name_matching)

    def to_json(self) -> dict:
        return asdict(self)

    def to_string(self):
        return dumps(self.to_json())

    def serialize(self) -> bytes:
        message = self.to_protobuf()
        return message.SerializeToString(deterministic=True)
