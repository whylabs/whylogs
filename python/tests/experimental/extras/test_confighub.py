import os
import shutil

from semantic_version import Version

from whylogs.experimental.extras.confighub import ConfigStore, LocalGitConfigStore


# TODO: use a temp directory under /tmp

def test_ctor():
    shutil.rmtree("/tmp/my_org", ignore_errors=True)

    # create empty repo at /tmp/my_org/my_model
    cs = LocalGitConfigStore("my_org", "my_model", "test")
    assert os.path.isdir("/tmp/my_org/my_model")
    assert os.path.isfile("/tmp/my_org/my_model/test")
    assert cs.get_available_versions() == [Version("0.0.0")]
    assert cs.get_version_of_latest() == Version("0.0.0")
    assert cs.get_latest() == ""

    # repeat with existing directory
    cs = LocalGitConfigStore("my_org", "my_model", "test")
    assert os.path.isdir("/tmp/my_org/my_model")
    assert os.path.isfile("/tmp/my_org/my_model/test")
    assert cs.get_available_versions() == [Version("0.0.0")]
    assert cs.get_version_of_latest() == Version("0.0.0")
    assert cs.get_latest() == ""
    shutil.rmtree("/tmp/my_org", ignore_errors=True)


def test_update_workflow():
    shutil.rmtree("/tmp/my_org", ignore_errors=True)

    # create empty repo
    cs = LocalGitConfigStore("my_org", "my_model", "test")

    # propose version 1.0.0 for review
    cur_ver = cs.get_version_of_latest()
    new_ver = cur_ver.next_major()
    cs.propose_version("contents\n", new_ver, "testing new version")
    assert cs.get_available_versions() == [Version("0.0.0")]  # 1.0.0 is not committed yet
    assert cs.get_version_of_latest() == Version("0.0.0")
    assert cs.get_version(new_ver).strip() == "contents"  # proposed available by version
    assert cs.get_latest() == ""  # 1.0.0 is not committed yet

    # commit 1.0.0
    cs.commit_version(new_ver)
    assert cs.get_latest().strip() == "contents"
    assert cs.get_available_versions() == [Version("0.0.0"), Version("1.0.0")]
    assert cs.get_version_of_latest() == Version("1.0.0")

    # propose 1.1.0
    new_ver = new_ver.next_minor()
    cs.propose_version("more contents\n", new_ver)
    assert cs.get_available_versions() == [Version("0.0.0"), Version("1.0.0")]
    assert cs.get_version_of_latest() == Version("1.0.0")
    # contents of old versions as expected
    assert cs.get_version(Version("0.0.0")).strip() == ""
    assert cs.get_version(Version("1.0.0")).strip() == "contents"
    assert cs.get_latest().strip() == "contents"
    assert cs.get_version(Version("1.1.0")).strip() == "more contents"

    # commit 1.1.0
    cs.commit_version(new_ver)
    assert cs.get_latest().strip() == "more contents"
    assert cs.get_available_versions() == [Version("0.0.0"), Version("1.0.0"), Version("1.1.0")]
    assert cs.get_version_of_latest() == Version("1.1.0")
    shutil.rmtree("/tmp/my_org", ignore_errors=True)
