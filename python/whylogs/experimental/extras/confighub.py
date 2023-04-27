import logging
import os
from abc import ABC, abstractmethod
from typing import List, Optional

from semantic_version import Version

logger = logging.getLogger(__name__)


class ConfigStore(ABC):
    def __init__(self, org_id: str, dataset_id: str, what: str, create: bool = False):
        """
        what in {"constraints", "validators", ... }
        """
        self._org_id = org_id
        self._dataset_id = dataset_id
        self._what = what
        if create and not self.exists():
            self.create()

    @abstractmethod
    def exists(self) -> bool:
        """Returns True iff the store exists"""
        return False

    @abstractmethod
    def create(self) -> None:
        """Create the store if it doesn't already exist"""
        pass

    @abstractmethod
    def get_latest(self) -> str:
        """Returns latest version of the config"""
        pass

    def get_version_of_latest(self) -> Version:
        """
        Returns the Version of the current config
        """
        return self.get_available_versions()[-1]

    @abstractmethod
    def get_version(self, ver: Version) -> str:
        """
        Returns the requested version of the config. ValueError on invalid/unavailable.
        """
        pass

    @abstractmethod
    def get_available_versions(self) -> List[Version]:
        """
        Returns available versions in ascending order
        """
        pass

    @abstractmethod
    def propose_version(self, config: str, new_version: Version, note: Optional[str] = None) -> None:
        """
        Submit config to review process with proposed new verison.
        ValueError if new_version <= latest
        """
        pass

    @abstractmethod
    def commit_version(self, new_version: Version) -> None:
        """
        Make the proposed config the latest version. ValueError if proposed new_version doesn't exist
        """
        pass


class LocalGitConfigStore(ConfigStore):
    def _run_commands(self, commands: List[str]) -> None:
        for command in commands:
            output = os.popen(f"cd {self._path}; {command}")
            logger.debug(f"{command}: {output.read()}")

    def __init__(self, org_id: str, dataset_id: str, what: str, create: bool = False, repo_path: str = "/tmp"):
        self._repo_path = repo_path
        self._path = f"{self._repo_path}/{org_id}/{dataset_id}"
        super().__init__(org_id, dataset_id, what, create)

    def create(self) -> None:
        if not os.path.isdir(self._path):
            os.makedirs(self._path)
            cmds = [
                "git init",
                f"touch {self._what}",
                f"git add {self._what}",
                'git commit -am "confighub initialization"',
                "git tag 0.0.0",
            ]
            self._run_commands(cmds)

    def exists(self) -> bool:
        return os.path.isdir(self._path)  # not the most throrough test...

    def _read_version(self, version: str) -> str:
        cmds = [
            f"git checkout {version}",
        ]
        self._run_commands(cmds)  # TODO: error checking
        with open(f"{self._path}/{self._what}", "r") as f:
            return f.read()

    def get_latest(self) -> str:
        return self._read_version("master")

    def get_version(self, ver: Version) -> str:
        return self._read_version(str(ver))

    def get_available_versions(self) -> List[Version]:
        output = os.popen(f"cd {self._path}; git tag")
        versions = [Version(v.strip()) for v in output.readlines()]
        versions.sort()
        return versions

    def propose_version(self, config: str, new_version: Version, note: Optional[str] = None) -> None:
        note = note or f"proposed {new_version}"
        cmds = [
            "git checkout master",
            f"git branch {new_version}",
            f"git checkout {new_version}",
        ]
        self._run_commands(cmds)
        with open(f"{self._path}/{self._what}", "w") as f:
            f.write(config)
        cmds = [
            f'git commit -am "{note}"',
        ]
        self._run_commands(cmds)

    # TODO: do we need an abandon version for if a propsed version is rejected?

    def commit_version(self, new_version: Version) -> None:
        # TODO: throw if new_version branch does not exist
        cmds = [
            f"git checkout {new_version}",
            "git merge --strategy=ours --no-commit master",
            f'git commit -m "promoting {new_version}"',
            "git checkout master",
            f"git merge {new_version}",
            f"git branch -d {new_version}",
            f"git tag {new_version}",
        ]
        self._run_commands(cmds)
