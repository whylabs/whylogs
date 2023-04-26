
from abc import ABC, abstractmethod
from typing import List

from semantic_version import Version


class ConfigStore(ABC):
    def __init__(self, org_id: str, dataset_id: str, what: str):
        """
        what in {"constraints", "validators", ... }
        """
        self._org_id = org_id
        self._dataset_id = dataset_id
        self._what = what

    @abstractmethod
    def get_latest(self) -> str:
        """Returns latest version of the config"""
        pass

    @abstractmethod
    def get_version(self, ver: Version) -> str:
        """Returns requested version of the config. ValueError on invalid/unavailable"""
        pass

    @abstractmethod
    def get_available_versions(self) -> List[Version]:  # List[Tuple[Version, timestamp]] ?
        """Returns available versions in ascending order, [1.0.0] if it's the first version"""
        pass

    @abstractmethod
    def propose_version(self, config: str, new_version: Version) -> None:
        """Submit config to review process with proposed new verison. ValueError if new_version <= latest"""
        pass

    @abstractmethod
    def commit_verison(self, config: str, new_version: Version) -> None:
        """Make config the latest version. ValueError if new_version < previous latest version"""
        pass
