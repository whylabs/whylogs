from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

ENV = os.environ.get("FLASK_ENV", "development")
DEBUG = os.environ.get("FLASK_DEBUG", 0)
SWAGGER_HOST = os.environ.get("SWAGGER_HOST", None)
SWAGGER_BASEPATH = os.environ.get("SWAGGER_BASEPATH", None)
SWAGGER_SCHEMES = os.environ.get("SWAGGER_SCHEMES", None)
WHYLABS_DEFAULT_DATASET_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID", None)
DATASET_URL = os.environ.get("DATASET_URL", None)
ROTATION_TIME = os.environ.get("ROTATION_TIME", None)
WHYLABS_CONFIG = os.environ.get("WHYLABS_CONFIG", None)

whylabs_session = None
whylabs_logger = None
df = None
