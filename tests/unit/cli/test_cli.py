import os

from click.testing import CliRunner

import whylogs.cli as client
from whylogs.cli.demo_cli import cli as democli


def test_init_empty_dir(tmp_path):

    runner = CliRunner()

    project_directory = tmp_path / "sub"
    project_directory.mkdir()
    with open(project_directory / "plain.txt", "a"):
        os.utime(project_directory / "plain.txt", None)
    result = runner.invoke(
        client.cli,
        ["init", "--project-dir", project_directory],
        input="no\n".format(project_directory),
    )

    assert result.exit_code == 0


def test_init_dir(tmp_path):

    runner = CliRunner()

    project_directory = tmp_path / "sub"
    project_directory.mkdir()

    result = runner.invoke(
        client.cli,
        ["-v", "init", "--project-dir", project_directory],
        input="yes\ntest\ntest\n{}\n\n".format(project_directory),
    )

    assert result.exit_code == 0


def test_demo_init_empty_dir(tmp_path):

    runner = CliRunner()

    project_directory = tmp_path / "sub"
    project_directory.mkdir()

    with open(project_directory / "plain.txt", "a"):
        os.utime(project_directory / "plain.txt", None)
    result = runner.invoke(
        democli,
        ["-v", "init", "--project-dir", project_directory],
        input="no\n".format(tmp_path),
    )

    assert result.exit_code == 0

    os.remove(project_directory / "plain.txt")


def test_demo_init_dir(tmpdir):

    runner = CliRunner()

    project_directory = tmpdir.mkdir("sub")

    result = runner.invoke(
        democli,
        ["-v", "init", "--project-dir", project_directory],
        input="yes\ntest\ntest\n{}\nyes\n1\nyes\n".format(project_directory),
    )

    assert result.exit_code == 0


def test_demo_noweb_dir(tmpdir):

    runner = CliRunner()

    project_directory = tmpdir.mkdir("sub")

    result = runner.invoke(
        democli,
        ["-v", "init", "--project-dir", project_directory],
        input="yes\ntest\ntest\n{}\nyes\n1\nno\n".format(project_directory),
    )

    assert result.exit_code == 0


def test_demo_no_init_prof_dir(tmpdir):

    runner = CliRunner()

    project_directory = tmpdir.mkdir("sub")

    result = runner.invoke(
        democli,
        ["-v", "init", "--project-dir", project_directory],
        input="yes\ntest\ntest\n{}\nno\n".format(project_directory),
    )

    assert result.exit_code == 0
