import datetime
import json
import os

import boto3
import botocore
import flask
import my_model as my_model_code
import numpy as np
import torch
import torchvision.transforms as transforms
from PIL import Image

from whylogs import get_or_create_session

prefix = "/opt/ml/"
# location of you checkpoint in sagemaker container
model_path = os.path.join(prefix, "model")


whylogs_session = get_or_create_session()
logger = whylogs_session.logger(dataset_name="my_deployed_model", dataset_timestamp=datetime.datetime.now(datetime.timezone.utc), with_rotation_time="5m")

# simple custom metric for embedding
embedding_centr = np.linalg.norm(np.ones(1024))


# loads the model into memory from disk and returns it
def model_fn(model_dir):

    model_config = {"backbone": "resnet", "outlayer": "C5", "model_path": os.path.join(model_dir, "my_checkpoint.pth.tar")}
    print("Model Config: {}", format(model_config))
    model = my_model_code.Image_Embeddings(model_config)

    try:
        # try to use gpu if available (there are prob faster ways to check)
        model.to(torch.device("cuda"))
        logger.log({"arch": "cuda"})
    except Exception:
        logger.log({"arch": "cpu"})
        return None

    return model


# pre-processing inputs
def input_fn(request_body):

    # Fetch the input data... assuming your data is in a bucket somewhere
    # it could be also be part of the request as a base64.
    request = request_body

    BUCKET_NAME = request["bucket_name"]
    KEY = os.path.join(request["image_path"])

    # whylog track file paths
    logger.log({"bucket": BUCKET_NAME})
    logger.log({"image_path": KEY})

    s3 = boto3.resource("s3")

    try:
        s3.Bucket(BUCKET_NAME).download_file(KEY, "temp_image.png")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print("The data does not exist.")
        else:
            raise
    # open image byte data and transform as necessary
    image_ = Image.open("temp_image.png").convert("RGB")

    # logger image
    logger.log_image(image_)

    resize = transforms.Resize(300)
    loader = transforms.Compose([resize, transforms.ToTensor()])
    img = loader(image_).float().unsqueeze(0)

    return img


# predicting the output
def predict_fn(input_object, model):

    # inference call
    try:
        # try to load data to the gpu
        input_object = input_object.to(torch.device("cuda"))
    except Exception:
        pass

    embedding = model(input_object).cpu().detach().numpy().flatten().tolist()

    return embedding


model = model_fn(model_path)

# The flask app for serving predictions
app = flask.Flask(__name__)


@app.route("/ping", methods=["GET"])
def ping():
    """Determine if the container is working and healthy. In this sample container, we declare
    it healthy if we can load the model successfully."""

    status = 200 if model else 404
    return flask.Response(response="\n", status=status, mimetype="application/json")


@app.route("/invocations", methods=["POST"])
def transformation():
    """Do an inference on a single batch of data."""

    if flask.request.content_type == "application/json":
        # load and preprocess image

        image = input_fn(flask.request.json)

    else:
        return flask.Response(
            response="This predictor only supports JSON data",
            status=415,
            mimetype="text/plain",
        )

    # embeded image
    embedding = predict_fn(image, model)
    logger.log({"distance_from_cntr": np.dot(embedding, embedding_centr)})
    result = {}
    result["embedding"] = embedding

    return flask.Response(response=json.dumps(result), status=200, mimetype="application/json")
