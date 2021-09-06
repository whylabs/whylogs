# Flask scikit-learn examples


## Environment variables management

Write a .env file with the following configuration

```bash
WHYLABS_DEFAULT_DATASET_ID=...
WHYLABS_API_KEY=...
WHYLABS_DEFAULT_ORG_ID=...
WHYLABS_API_ENDPOINT=...
```
## Train the model

__Download IRIS dataset.__

1. Configure kaggle credentials with the following instructions: https://github.com/Kaggle/kaggle-api#api-credentials
2. Run download_iris.sh

__Train an SVM classifier__

1. Configure dependencies in an environment (e.g. can use conda).
2. Run train.py.

## Build the image

```bash
docker build --build-arg PYTHON_VERSION=3.7 -t whylogs-flask .
```

## Run the image

```bash
docker run --rm -p 5000:5000 -v $(pwd):/app  whylogs-flask
```

## Swagger Documentation

Go to http://0.0.0.0:5000/apidocs.