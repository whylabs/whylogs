import os
from tempfile import TemporaryDirectory

from semantic_version import Version

from whylogs.experimental.extras.confighub import LocalGitConfigStore


def test_ctor():
    with TemporaryDirectory(dir="/tmp") as prefix:
        # create empty repo at prefix/my_org/my_model
        cs = LocalGitConfigStore("my_org", "my_model", "test", repo_path=prefix)
        assert not cs.exists()
        cs.create()
        assert cs.exists()
        assert os.path.isdir(f"{prefix}/my_org/my_model")
        assert os.path.isfile(f"{prefix}/my_org/my_model/test")
        assert cs.get_available_versions() == [Version("0.0.0")]
        assert cs.get_version_of_latest() == Version("0.0.0")
        assert cs.get_latest() == ""

        # repeat with existing directory
        cs = LocalGitConfigStore("my_org", "my_model", "test", repo_path=prefix)
        assert cs.exists()
        assert os.path.isdir(f"{prefix}/my_org/my_model")
        assert os.path.isfile(f"{prefix}/my_org/my_model/test")
        assert cs.get_available_versions() == [Version("0.0.0")]
        assert cs.get_version_of_latest() == Version("0.0.0")
        assert cs.get_latest() == ""


def test_update_workflow():
    with TemporaryDirectory(dir="/tmp") as prefix:
        # create empty repo
        cs = LocalGitConfigStore("my_org", "my_model", "test", True, prefix)
        assert cs.exists()

        # propose version 1.0.0 for review
        cur_ver = cs.get_version_of_latest()
        new_ver = cur_ver.next_major()
        cs.propose_version("contents\n", new_ver, "testing new version")
        assert cs.get_available_versions() == [Version("0.0.0")]  # 1.0.0 is not committed yet
        assert cs.get_version_of_latest() == Version("0.0.0")
        assert cs.get_version(new_ver).strip() == "contents"  # proposed available by version
        assert cs.get_latest() == ""  # 1.0.0 is not committed yet

        # commit 1.0.0 after approval
        cs.commit_version(new_ver)
        assert cs.get_latest().strip() == "contents"
        assert cs.get_available_versions() == [Version("0.0.0"), Version("1.0.0")]
        assert cs.get_version_of_latest() == Version("1.0.0")

        # propose 1.1.0 for review
        new_ver = new_ver.next_minor()
        cs.propose_version("more contents\n", new_ver)
        assert cs.get_available_versions() == [Version("0.0.0"), Version("1.0.0")]
        assert cs.get_version_of_latest() == Version("1.0.0")
        # contents of old versions as expected
        assert cs.get_version(Version("0.0.0")).strip() == ""
        assert cs.get_version(Version("1.0.0")).strip() == "contents"
        assert cs.get_latest().strip() == "contents"
        assert cs.get_version(Version("1.1.0")).strip() == "more contents"

        # commit 1.1.0 after approval
        cs.commit_version(new_ver)
        assert cs.get_latest().strip() == "more contents"
        assert cs.get_available_versions() == [Version("0.0.0"), Version("1.0.0"), Version("1.1.0")]
        assert cs.get_version_of_latest() == Version("1.1.0")
