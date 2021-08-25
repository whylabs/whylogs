import io
import json
import datetime
from  smart_open import  open

import torchvision.transforms as transforms
from flask import Flask, jsonify, request
from PIL import Image
from torchvision import models

from whylogs import get_or_create_session

session = get_or_create_session()
logger = session.logger(dataset_name="my_deployed_model", 
                        dataset_timestamp= datetime.datetime.now(datetime.timezone.utc),
                        with_rotation_time="30s")


app = Flask(__name__)
imagenet_class_index = json.load(open("imagenet_class_index.json"))
model = models.densenet121(pretrained=True)
model.eval()


def transform_image(image_bytes):
    my_transforms = transforms.Compose(
        [transforms.Resize(255), transforms.CenterCrop(224), transforms.ToTensor(), transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])]
    )
    image = Image.open(io.BytesIO(image_bytes))
    logger.log_image(image)

    return my_transforms(image).unsqueeze(0)


def get_prediction(image_bytes):
    tensor = transform_image(image_bytes=image_bytes)

    logger.log({"batch_size": tensor.shape[0]})
    outputs = model.forward(tensor)

    conf, y_hat = outputs.max(1)
    logger.log({"confidence":conf.item()})
    predicted_idx = str(y_hat.item())
    return imagenet_class_index[predicted_idx]

@app.route("/predict", methods=["POST"])
def predict():
    
    if request.method == "POST":
        filepath = request.json["file"]
        logger.log({"file":filepath})
        with open(filepath,"rb") as file:
            img_bytes = file.read()
        class_id, class_name = get_prediction(image_bytes=img_bytes)
        logger.log({"class_id":class_id})
        logger.log({"class_name":class_name})

        return jsonify({"class_id": class_id, "class_name": class_name})


if __name__ == "__main__":
    app.run(debug=True)
