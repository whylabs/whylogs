from flasgger import Swagger
import yaml


def init_swagger(app):

    swagger_dict = yaml.safe_load(open("swagger.yaml"))
    swagger_dict["host"] = app.config["SWAGGER_HOST"]
    swagger_dict["basePath"] = app.config["SWAGGER_BASEPATH"]
    swagger_dict["schemes"] = app.config["SWAGGER_SCHEMES"]

    swagger = Swagger(app, template=swagger_dict)
    return swagger
