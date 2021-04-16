import os
import re
import sys

import click

from whylogs.app import SessionConfig, WriterConfig
from whylogs.app.config import WHYLOGS_YML
from whylogs.cli.utils import echo

from .cli_text import (
    BEGIN_WORKFLOW,
    DOING_NOTHING_ABORTING,
    EMPTY_PATH_WARNING,
    INTRO_MESSAGE,
    OVERRIDE_CONFIRM,
    PIPELINE_DESCRIPTION,
    PROJECT_DESCRIPTION,
    PROJECT_NAME_PROMPT,
)

LENDING_CLUB_CSV = "lending_club_1000.csv"

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
    help="The root of the new whylogs profiling project.",
)
def init(project_dir):
    """
    Initialize and configure a new whylogs project.

    This guided input walks the user through setting up a new project and also
    onboards a new developer in an existing project.

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
    config_yml = os.path.join(project_dir, WHYLOGS_YML)
    with open(file=config_yml, mode="wt") as f:
        session_config.to_yaml(f)
    echo(f"Config YAML file was written to: {config_yml}\n")
    echo(
        "To get started with a whylogs session, use whylogs.get_or_created_session() in the project folder.",
        fg="green",
    )
