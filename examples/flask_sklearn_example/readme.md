# Flask scikit-learn examples


## Environment variables management

```bash
WHYLABS_DEFAULT_DATASET_ID=...
WHYLABS_API_KEY=...
WHYLABS_DEFAULT_ORG_ID=...
WHYLABS_API_ENDPOINT=...
```

## Build the image

```bash
docker build --build-arg PYTHON_VERSION=3.7 -t whylabs-flask .
```

## Run the image

```bash
docker run --rm -p 5000:5000 -v $(pwd):/app  whylogs-flask
```


## Train the model

__Download IRIS dataset.__

1. Configure kaggle credentials: https://github.com/Kaggle/kaggle-api
2. Run download_iris.sh

__Train an SVM classifier__

1. Configure dependencies in an environment (e.g. can use conda).
2. Run train.py.