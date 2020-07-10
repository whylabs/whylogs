import io
import os
import re
import sys

import click
from click import secho as echo

INTRO_MESSAGE = """
 __     __     __  __     __  __        __         ______     ______     ______    
/\ \  _ \ \   /\ \_\ \   /\ \_\ \      /\ \       /\  __ \   /\  ___\   /\  ___\   
\ \ \/ ".\ \  \ \  __ \  \ \____ \     \ \ \____  \ \ \/\ \  \ \ \__ \  \ \___  \  
 \ \__/".~\_\  \ \_\ \_\  \/\_____\     \ \_____\  \ \_____\  \ \_____\  \/\_____\ 
  \/_/   \/_/   \/_/\/_/   \/_____/      \/_____/   \/_____/   \/_____/   \/_____/ 
                                                                                   
"""

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
        echo('WARNING: we will override the content in the non-empty path', fg='yellow')

    if not click.confirm("Would you like to proceed with the above path?", default=False, show_default=True):
        echo("Doing nothing. Aborting")
        sys.exit(0)

    echo('Great. We will now generate the default configuration for WhyLogs')
    echo("We'll need a few details from you before we can proceed")
    dataset_name = click.prompt('Dataset name (alphanumeric, dash, and underscore characters only)',
                                type=DatasetParamType())

    echo(f'Using dataset name: {dataset_name}', fg='green')
    if click.confirm('Would you like to run an initial profiling job?', default=True):
        echo("Select data source:")
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
            if not click.confirm('Profile path already exists. This will override existing data', default=True):
                echo('Abort profiling')
                sys.exit(0)
            else:
                echo('Previous profile data will be overridden', fg='yellow')
        echo('WhyLogs can break down the data by time for you')
        echo('This will enable users to run time-based analysis')
        datetime_column = click.prompt('What is your datetime column?',
                                       type=click.STRING,
                                       default='')
        datetime_format = ''
        if not datetime_column:
            echo('Skip date time handling')
        else:
            datetime_format = click.prompt('What is the format of the column? Leave blank to use datetimeutil to parse',
                                           default='')
        if datetime_format:
            echo(f'Date time format used: {datetime_format}')
        echo('Run profiling')
        echo('Successful. You can find the result under "profile" path')
