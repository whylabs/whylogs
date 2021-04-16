import io
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import webbrowser

import click
import pandas as pd

from whylogs import __version__ as whylogs_version
from whylogs.app import SessionConfig, WriterConfig
from whylogs.app.config import WHYLOGS_YML
from whylogs.app.session import session_from_config
from whylogs.cli.utils import echo

from .cli_text import (
    BEGIN_WORKFLOW,
    DATA_SOURCE_MESSAGE,
    DOING_NOTHING_ABORTING,
    DONE,
    EMPTY_PATH_WARNING,
    GENERATE_NOTEBOOKS,
    INITIAL_PROFILING_CONFIRM,
    INTRO_MESSAGE,
    OBSERVATORY_EXPLANATION,
    OVERRIDE_CONFIRM,
    PIPELINE_DESCRIPTION,
    PROJECT_DESCRIPTION,
    PROJECT_NAME_PROMPT,
    RUN_PROFILING,
)

_LENDING_CLUB_CSV = "lending_club_1000.csv"
_EXAMPLE_REPO = "https://github.com/whylabs/whylogs-examples.git"


def _set_up_logger():
    # Log to console with a simple formatter; used by CLI
    formatter = logging.Formatter("%(message)s")
    handler = logging.StreamHandler()

    handler.setFormatter(formatter)
    module_logger = logging.getLogger("whylogs")
    module_logger.addHandler(handler)
    module_logger.setLevel(level=logging.WARNING)

    return module_logger


NAME_FORMAT = re.compile(r"^(\w|-|_)+$")


class NameParamType(click.ParamType):
    def convert(self, value, param, ctx):
        if NAME_FORMAT.fullmatch(value) is None:
            raise click.BadParameter("must contain only alphanumeric, underscore and dash characters")
        return value


@click.command()
@click.option(
    "--project-dir",
    "-d",
    default="./",
    help="The root of the new whylogs demo project.",
)
def init(project_dir):
    """
    Initialize and configure a new whylogs project.

    This guided input walks the user through setting up a new project and also
    on-boards a new developer in an existing project.

    It scaffolds directories, sets up notebooks, creates a project file, and
    appends to a `.gitignore` file.
    """
    echo(INTRO_MESSAGE, fg="green")
    project_dir = os.path.abspath(project_dir)
    echo(f"Project path: {project_dir}")

    is_project_dir_empty = len(os.listdir(path=project_dir)) == 0
    if not is_project_dir_empty:
        echo(EMPTY_PATH_WARNING, fg="yellow")

    if not click.confirm(OVERRIDE_CONFIRM, default=False, show_default=True):
        echo(DOING_NOTHING_ABORTING)
        sys.exit(0)
    os.chdir(project_dir)

    echo(BEGIN_WORKFLOW)
    echo(PROJECT_DESCRIPTION)
    project_name = click.prompt(PROJECT_NAME_PROMPT, type=NameParamType())
    echo(f"Using project name: {project_name}", fg="green")
    echo(PIPELINE_DESCRIPTION)
    pipeline_name = click.prompt(
        "Pipeline name (leave blank for default pipeline name)",
        type=NameParamType(),
        default="default-pipeline",
    )
    echo(f"Using pipeline name: {pipeline_name}", fg="green")
    output_path = click.prompt("Specify the whylogs output path", default="output", show_default=True)
    echo(f"Using output path: {output_path}", fg="green")
    writer = WriterConfig("local", ["all"], output_path)
    session_config = SessionConfig(project_name, pipeline_name, writers=[writer], verbose=False)

    echo("Adding example notebooks to your workspace")
    git = shutil.which("git")
    if git is None:
        echo("We can't seem to find git utility on your system. We'll have kip this step")
        echo("You can check out our repo on: https://github.com/whylabs/whylogs-examples")
    else:
        # do git checkout here
        tmp_path = tempfile.mkdtemp("profiler")
        subprocess.run(
            [git, "clone", "--depth", "1", _EXAMPLE_REPO],
            cwd=tmp_path,
            check=True,
        )

        example_python = os.path.join(tmp_path, "whylogs-examples", "python")
        files = os.listdir(example_python)
        for f in files:
            shutil.copy(os.path.join(example_python, f), os.path.join(project_dir, f))
        shutil.rmtree(tmp_path)

    config_yml = os.path.join(project_dir, WHYLOGS_YML)
    with open(file=config_yml, mode="wt") as f:
        session_config.to_yaml(f)
    echo(f"Config YAML file was written to: {config_yml}\n")

    if click.confirm(INITIAL_PROFILING_CONFIRM, default=True):
        echo(DATA_SOURCE_MESSAGE)
        choices = [
            "CSV on the file system",
        ]
        for i in range(len(choices)):
            echo(f"\t{i + 1}. {choices[i]}")
        choice = click.prompt("", type=click.IntRange(min=1, max=len(choices)))
        assert choice == 1
        profile_csv(session_config, project_dir)
        echo(
            f"You should find the whylogs output under: {os.path.join(project_dir, output_path, project_name)}",
            fg="green",
        )

        echo(GENERATE_NOTEBOOKS)
        # Hack: Takes first all numeric directory as generated datetime for now
        output_full_path = os.path.join(project_dir, output_path)
        generated_datetime = list(filter(lambda x: re.match("[0-9]*", x), os.listdir(output_full_path)))[0]
        full_output_path = os.path.join(output_path, generated_datetime)
        echo(f"You should find the output under: {full_output_path}")

        echo(OBSERVATORY_EXPLANATION)
        echo("Your original data (CSV file) will remain locally.")
        should_open = click.confirm(
            "Would you like to proceed to WhyLabs Playground to see how our data visualization works?",
            default=False,
            show_default=True,
        )
        if should_open:
            webbrowser.open("https://try.whylabsapp.com/?utm_source=whylogs")
        echo(DONE)
    else:
        echo("Skip initial profiling and notebook generation")
        echo(DONE)


def profile_csv(session_config: SessionConfig, project_dir: str) -> str:
    file: io.TextIOWrapper = click.prompt(
        "CSV input path (leave blank to use our demo dataset)",
        type=click.File(mode="rt"),
        default=io.StringIO(),
        show_default=False,
    )
    if type(file) is io.StringIO:
        echo("Using the demo Lending Club Data (1K randomized samples)", fg="green")
        destination_csv = os.path.join(project_dir, _LENDING_CLUB_CSV)
        full_input = os.path.realpath(destination_csv)
    else:
        file.close()
        full_input = os.path.realpath(file.name)
    echo(f"Input file: {full_input}")
    echo(RUN_PROFILING)
    session = session_from_config(session_config)
    df = pd.read_csv(full_input)
    session.log_dataframe(df)
    session.close()
    return full_input


@click.group()
@click.version_option(version=whylogs_version)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Set whylogs CLI to use verbose output.",
)
def cli(verbose):
    """
    Welcome to whylogs Demo CLI!

    Supported commands:

    - whylogs-demo init : create a demo whylogs project with example data and notebooks
    """
    logger = _set_up_logger()
    if verbose:
        logger.setLevel(logging.DEBUG)


cli.add_command(init)


def main():
    _set_up_logger()
    cli()


if __name__ == "__main__":
    main()
