import io
import os
import re
import sys

import click
from click import secho as echo
from whylabs.logs.cli.generate_notebooks import generate_notebooks

from whylabs.logs.app.config import SessionConfig, WriterConfig
from whylabs.logs.app.session import session_from_config
from whylabs.logs.cli.cli_text import *
import pandas as pd

from whylabs.logs.cli.generate_notebooks import generate_notebooks


def echo(message: typing.Union[str, list], **styles):
    if isinstance(message, list):
        for msg in message:
            click.secho(msg, **styles)
    else:
        click.secho(message, **styles)


NAME_FORMAT = re.compile(r'^(\w|-|_)+$')


class NameParamType(click.ParamType):
    def convert(self, value, param, ctx):
        if NAME_FORMAT.fullmatch(value) is None:
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
    os.chdir(project_dir)

    echo(BEGIN_WORKFLOW)
    project_name = click.prompt(PROJECT_NAME_PROMPT, type=NameParamType())
    echo(f'Using project name : {project_name}', fg='green')
    pipeline_name = click.prompt('Pipeline name (leave blank for default pipeline name)', type=NameParamType(),
                                 default='default-pipeline')
    echo(f'Using pipeline name: {pipeline_name}', fg='green')
    output_path = click.prompt('Specify the output path', default='output')
    echo(f'Using output path: {output_path}')
    writer = WriterConfig('local', ['all'], output_path)
    session_config = SessionConfig(project_name, pipeline_name, verbose=False, writers=[writer])
    config_yml = os.path.join(project_dir, 'whylogs.yml')
    with open(file=config_yml, mode='w') as f:
        session_config.to_yaml(f)
    echo(f'Config YAML file was written to: {config_yml}\n')

    if click.confirm(INITIAL_PROFILING_CONFIRM, default=True):
        echo(DATA_SOURCE_MESSAGE)
        choices = [
            'CSV on the file system',
        ]
        for i in range(len(choices)):
            echo(f'\t{i + 1}. {choices[i]}')
        choice = click.prompt('', type=click.IntRange(min=1, max=len(choices)))
        assert choice == 1
        profile_csv(project_dir, session_config)
        echo(f'You should find the output under: {os.path.join(project_dir, "output")}')
        echo('Generate notebooks')
        echo('Successful. You can find the notebooks under "notebooks" path')

        echo(DONE)
    else:
        echo(DONE)


def profile_csv(project_dir: str, session_config: SessionConfig) -> str:
    file: io.TextIOWrapper = click.prompt('CSV input path', type=click.File())
    file.close()
    full_input = os.path.realpath(file.name)
    echo(f'Input file: {full_input}')
    output_path = os.path.join(project_dir, 'whylogs')
    if os.path.exists(output_path):
        if not click.confirm(PROFILE_OVERRIDE_CONFIRM, default=True):
            echo('Abort profiling')
            sys.exit(0)
        else:
            datetime_format = click.prompt('What is the format of the column? Leave blank to use datetimeutil to parse',
                                           default='')
        if datetime_format:
            echo(f'Date time format used: {datetime_format}')
        echo('Run profiling')
        echo('Successful. You can find the result under "profile" path')
        echo('Generate notebooks')
        generate_notebooks(project_dir,
                           {"INPUT_PATH": full_path,
                            "DATASET_NAME": dataset_name,
                            "PROFILE_PATH": output_path,
                            "DATETIME_COLUMN": datetime_column,
                            "DATETIME_FORMAT": datetime_format})
        echo('Successful. You can find the notebooks under "notebooks" path')
