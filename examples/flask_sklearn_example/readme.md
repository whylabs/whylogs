# Flask scikit-learn example

This example can be used runs a Flask application that hosts a scikit-learn model serialized with joblib.

## Whylabs configuration

Adapt the .whylabs.yaml:

```yaml
project: example-project
pipeline: example-pipeline
verbose: false
writers:
- data_collection_consent: true
  formats: ['protobuf']
  output_path: whylabs
  type: whylabs
```

## Environment variables management

Write a .env file with the following configuration:

```bash
# Flask
FLASK_ENV=development
FLASK_DEBUG=1
FLASK_APP=autoapp.py
MODEL_PATH=model.joblib

# Swagger Documentation
SWAGGER_HOST=0.0.0.0:5000
SWAGGER_BASEPATH=/api/v1
SWAGGER_SCHEMES={"http"}

# WhyLabs
WHYLABS_CONFIG=.whylabs.yaml
WHYLABS_API_KEY=...
WHYLABS_DEFAULT_ORG_ID=...
WHYLABS_DEFAULT_DATASET_ID=model-1
WHYLABS_API_ENDPOINT=https://api.whylabsapp.com
WHYLABS_N_ATTEMPS=3

# Whylabs session
DATASET_NAME=this_is_my_dataset
ROTATION_TIME=1h
DATASET_URL=dataset/iris.csv
```
## Train the model

__Download IRIS dataset.__

1. Configure kaggle credentials with the following instructions: https://github.com/Kaggle/kaggle-api#api-credentials
2. Run download_iris.sh

__Train an SVM classifier__

1. Configure dependencies in an environment (e.g. can use conda).
2. Run train.py.

__Note__: To be able to run __train.py__ you need to install kaggle.

## Build the image

```bash
docker build --build-arg PYTHON_VERSION=3.7 -t whylabs-flask .
```

## Run the image

```bash
docker run --rm -p 5000:5000 -v $(pwd):/app  whylabs-flask
```

## Swagger Documentation

To open Swagger UI go to http://0.0.0.0:5000/apidocs.