from .model_wrapper import ModelWrapper


def _load_pyfunc(path: str):
    import mlflow

    # Load the Sklearn function
    model = mlflow.sklearn._load_pyfunc(path)

    return ModelWrapper(model)
