import io
import os
import re
import sys

import click
import typing

from whylabs.logs.cli.cli_text import *


def echo(message: typing.Union[str, list], **styles):
    if isinstance(message, list):
        for msg in message:
            click.secho(msg, **styles)
    else:
        click.secho(message, **styles)


DATASET_NAME_FOMRAT = re.compile(r'^\w+$')


class DatasetParamType(click.ParamType):
    def convert(self, value, param, ctx):
        if DATASET_NAME_FOMRAT.fullmatch(value) is None:
            raise click.BadParameter('must contain only alphanumeric, underscore and dash characters')
        return value


@click.command()
@click.option(
    "--project-dir",
    "-d",
    default="./",
    help="The root of the new WhyLogs profiling project.",
)
def init(project_dir):
    """
    Initialize and configure a new WhyLogs project.

    This guided input walks the user through setting up a new project and also
    onboards a new developer in an existing project.

    It scaffolds directories, sets up notebooks, creates a project file, and
    appends to a `.gitignore` file.
    """
    echo(INTRO_MESSAGE, fg='green')
    project_dir = os.path.abspath(project_dir)
    echo(f'Project path: {project_dir}')

    is_project_dir_empty = len(os.listdir(path=project_dir)) == 0
    if not is_project_dir_empty:
        echo(EMPTY_PATH_WARNING, fg='yellow')

    if not click.confirm(OVERRIDE_CONFIRM, default=False, show_default=True):
        echo(DOING_NOTHING_ABORTING)
        sys.exit(0)

    echo(BEGIN_WORKFLOW)
    dataset_name = click.prompt(DATASET_NAME_PROMPT, type=DatasetParamType())
    echo(f'Using dataset name: {dataset_name}', fg='green')

    echo(DATETIME_EXPLANATION)
    datetime_column = click.prompt(DATETIME_COLUMN_PROMPT, type=click.STRING, default='')
    datetime_format = ''
    if not datetime_column:
        echo(SKIP_DATETIME)
    else:
        datetime_format = click.prompt(DATETIME_FORMAT_PROMPT, default='')
    if datetime_format:
        echo(f'Date time format used: {datetime_format}')

    if click.confirm(INITIAL_PROFILING_CONFIRM, default=True):
        echo(DATA_SOURCE_MESSAGE)
        choices = [
            'single CSV on the file system',
            # 'single CSV on S3',
            # 'multiple CSVs on the file system',
            # 'multiple CSVs on S3',
        ]
        for i in range(len(choices)):
            echo(f'\t{i + 1}. {choices[i]}')
        choice = click.prompt('', type=click.IntRange(min=1, max=len(choices)))
        assert choice == 1
        file: io.TextIOWrapper = click.prompt('CSV input path', type=click.File())
        file.close()
        full_path = os.path.realpath(file.name)
        echo(f'Input file: {full_path}')
        output_path = os.path.join(project_dir, 'profile')
        if os.path.exists(output_path):
            if not click.confirm(PROFILE_OVERRIDE_CONFIRM, default=True):
                echo('Abort profiling')
                sys.exit(0)
            else:
                echo(DATA_WILL_BE_OVERRIDDEN, fg='yellow')

        echo(RUN_PROFILING)
        echo(DONE)
