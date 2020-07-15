INTRO_MESSAGE = """
██╗    ██╗██╗  ██╗██╗   ██╗██╗      ██████╗  ██████╗ ███████╗
██║    ██║██║  ██║╚██╗ ██╔╝██║     ██╔═══██╗██╔════╝ ██╔════╝
██║ █╗ ██║███████║ ╚████╔╝ ██║     ██║   ██║██║  ███╗███████╗
██║███╗██║██╔══██║  ╚██╔╝  ██║     ██║   ██║██║   ██║╚════██║
╚███╔███╔╝██║  ██║   ██║   ███████╗╚██████╔╝╚██████╔╝███████║
 ╚══╝╚══╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝ ╚═════╝  ╚═════╝ ╚══════╝
                       / \__
                      (    @\___
                      /         O
                     /   (_____/
                    /_____/   U

Welcome to WhyLogs!
"""

DOING_NOTHING_ABORTING = 'Doing nothing. Aborting'
OVERRIDE_CONFIRM = 'Would you like to proceed with the above path?'

EMPTY_PATH_WARNING = 'WARNING: we will override the content in the non-empty path'

BEGIN_WORKFLOW = """
Great. We will now generate the default configuration for WhyLogs'
We'll need a few details from you before we can proceed
"""

DATASET_NAME_PROMPT = 'Dataset name (alphanumeric, dash, and underscore characters only)'

DATETIME_EXPLANATION = """
WhyLogs can break down the data by time for you
This will enable users to run time-based analysis
"""

DATETIME_COLUMN_PROMPT = 'What is the name of the datetime feature (leave blank to skip)?'

SKIP_DATETIME = 'Skip grouping by datetime'

DATETIME_FORMAT_PROMPT = 'What is the format of the column? Leave blank to use datetimeutil to parse'

INITIAL_PROFILING_CONFIRM = 'Would you like to run an initial profiling job?'

DATA_SOURCE_MESSAGE = 'Select data source:'

PROFILE_OVERRIDE_CONFIRM = 'Profile path already exists. This will override existing data'

DATA_WILL_BE_OVERRIDDEN = 'Previous profile data will be overridden'

RUN_PROFILING = 'Run profiling'

DONE = 'Done'
