INTRO_MESSAGE = """
██╗    ██╗██╗  ██╗██╗   ██╗██╗      ██████╗  ██████╗ ███████╗
██║    ██║██║  ██║╚██╗ ██╔╝██║     ██╔═══██╗██╔════╝ ██╔════╝
██║ █╗ ██║███████║ ╚████╔╝ ██║     ██║   ██║██║  ███╗███████╗
██║███╗██║██╔══██║  ╚██╔╝  ██║     ██║   ██║██║   ██║╚════██║
╚███╔███╔╝██║  ██║   ██║   ███████╗╚██████╔╝╚██████╔╝███████║
 ╚══╝╚══╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝ ╚═════╝  ╚═════╝ ╚══════╝
                       / \\__
                      (    @\\___
                      /         O
                     /   (_____/
                    /_____/   U

Welcome to whylogs!

Join us our community slack at  http://join.slack.whylabs.ai/

This CLI will guide you through initializing a basic whylogs configurations.

"""

DOING_NOTHING_ABORTING = "Doing nothing. Aborting"
OVERRIDE_CONFIRM = "Would you like to proceed with the above path?"

EMPTY_PATH_WARNING = "WARNING: we will override the content in the non-empty path"

BEGIN_WORKFLOW = """
Great. We will now generate the default configuration for whylogs'
We'll need a few details from you before we can proceed
"""

PIPELINE_DESCRIPTION = (
    '"Pipeline" is a series of one or multiple datasets to build a single model or application. A ' "project might contain multiple pipelines "
)

PROJECT_NAME_PROMPT = "Project name (alphanumeric, dash, and underscore characters only)"

PROJECT_DESCRIPTION = '"Project" is a collection of related datasets that are used for multiple models or applications.'

DATETIME_EXPLANATION = """
whylogs can break down the data by time for you
This will enable users to run time-based analysis
"""

DATETIME_COLUMN_PROMPT = "What is the name of the datetime feature (leave blank to skip)?"

SKIP_DATETIME = "Skip grouping by datetime"

DATETIME_FORMAT_PROMPT = "What is the format of the column? Leave blank to use datetimeutil to parse"

INITIAL_PROFILING_CONFIRM = "Would you like to run an initial profiling job?"

DATA_SOURCE_MESSAGE = "Select data source:"

PROFILE_OVERRIDE_CONFIRM = "Profile path already exists. This will override existing data"

DATA_WILL_BE_OVERRIDDEN = "Previous profile data will be overridden"

OBSERVATORY_EXPLANATION = (
    "WhyLabs Observatory can visualize your statistics. This will require the CLI to upload \n"
    "your statistics to WhyLabs endpoint. Your original data (CSV file) will remain locally."
)

RUN_PROFILING = "Run whylogs profiling..."

GENERATE_NOTEBOOKS = "Generate Jupyter notebooks"

DONE = "Done"
