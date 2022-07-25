import os

ENV = os.environ.get("FLASK_ENV", "development")
DEBUG = os.environ.get("FLASK_DEBUG", 0)
SWAGGER_HOST = os.environ.get("SWAGGER_HOST", None)
SWAGGER_BASEPATH = os.environ.get("SWAGGER_BASEPATH", None)
SWAGGER_SCHEMES = os.environ.get("SWAGGER_SCHEMES", None)
DATASET_URL = os.environ.get("DATASET_URL", None)
ROTATION_TIME = os.environ.get("ROTATION_TIME", 15)
ROTATION_UNITS = os.environ.get("ROTATION_UNITS", "M")

WHYLABS_CONFIG = os.environ.get("WHYLABS_CONFIG", None)
WHYLABS_DEFAULT_DATASET_ID = os.environ.get("WHYLABS_DEFAULT_DATASET_ID", None)
WHYLABS_API_KEY = os.environ.get("WHYLABS_API_KEY", None)
WHYLABS_DEFAULT_ORG_ID = os.environ.get("WHYLABS_DEFAULT_ORG_ID", None)

df = None
