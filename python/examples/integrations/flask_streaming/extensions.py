import yaml
from flasgger import Swagger  # type: ignore


def init_swagger(app):  # type: ignore

    swagger_dict = yaml.safe_load(open("swagger.yaml"))
    swagger_dict["host"] = app.config["SWAGGER_HOST"]
    swagger_dict["basePath"] = app.config["SWAGGER_BASEPATH"]
    swagger_dict["schemes"] = app.config["SWAGGER_SCHEMES"]

    swagger = Swagger(app, template=swagger_dict)
    return swagger
