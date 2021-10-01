import os

ENV = os.environ.get("FLASK_ENV", "development")
DEBUG = os.environ.get("FLASK_DEBUG", 0)
WHYLABS_DEFAULT_DATASET_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID", None)
DATASET_URL = os.environ.get("DATASET_URL", None)
ROTATION_TIME = os.environ.get("ROTATION_TIME", None)
WHYLABS_CONFIG = os.environ.get("WHYLABS_CONFIG", None)
WHYLABS_N_ATTEMPS = os.environ.get("WHYLABS_N_ATTEMPS", 3)

whylabs_session = None
whylabs_logger = None
df = None
