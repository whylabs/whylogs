import os

ENV = os.environ.get("FLASK_ENV", "development")
DEBUG = os.environ.get("FLASK_DEBUG", 0)
SWAGGER_HOST = os.environ.get("SWAGGER_HOST", None)
SWAGGER_BASEPATH = os.environ.get("SWAGGER_BASEPATH", None)
SWAGGER_SCHEMES = os.environ.get("SWAGGER_SCHEMES", None)
DATASET_URL = os.environ.get("DATASET_URL", None)
ROTATION_TIME = os.environ.get("ROTATION_TIME", 15)
ROTATION_UNITS = os.environ.get("ROTATION_UNITS", "M")

df = None
