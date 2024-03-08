# 🍱 Monitor ML Models Managed and Served with BentoML

BentoML is an open-source framework that provides a model registry for versioning ML models and a model serving platform for deploying models to production with support for most major machine learning frameworks, such as scikit-learn, PyTorch, Tensorflow, Keras, XGBoost, and more.

whylogs can easily integrate with BentoML machine learning projects to perform data and ML monitoring in real-time.

## 📙 Example Overview:

This example shows how to write whylogs data profiles to the [WhyLabs](https://whylabs.ai/) platform for monitoring a machine learning model with BentoML and scikit-learn.

There are three notable files in this example:

- `train.py` - Trains and saves a versioned kNN model with scikit-learn, BentoML, and the iris dataset.
- `service_thread_logger.py` - Creates a BentoML service that uses our thread based rolling logger
- `service_process_logger.py` - Creates a BentoML service that uses our process based rolling logger

## 🛠️ How to implement ML monitoring for BentoML with whylogs and WhyLabs:

After running the training script with `python train.py`, you'll see a message similar to "`Model saved: Model(tag="iris_knn:fhfsvifgrsrtjlg6")`". This is the versioned model saved to the BentoML local model store.

> Note: It can be good practice to log the training data as a static reference profile to use as a baseline to detect data drift. For more information, check out the [whylogs documentation](https://whylogs.readthedocs.io/en/latest/examples/integrations/writers/Writing_Reference_Profiles_to_WhyLabs.html).

Next, In `service.py`, we'll get ready to serve and monitor our latest saved model.

To get the WhyLabs API key and org-id, go to the [WhyLabs dashboard](https://hub.whylabsapp.com/) and click on the "Access Tokens" section in the settings menu. Model-ids are found under the "Project Management" tab in the same section.

```python
import os

# Set this in the server file or directly in your env (for testing). In production it should be set
# in your environment without including the key in your python code.
os.environ["WHYLABS_API_KEY"] = "APIKEY"
```

> Note: A best practice for software deployment is to have these environment variables always set on the machine/environment level (such as per the CI/QA machine, a Kubernetes Pod, etc.) so it is possible to leave code untouched and persist it correctly according to the environment you execute it.

Now that our bentoML runner and environment variables are set, we can create an API endpoint with BentoML and set up model and data monitoring with whylogs and WhyLabs. We'll Log the data, predicted class, and model probability score. Create a Service instance and an endpoint to recieve inference requests.

```python
import whylogs

iris_clf_runner = bentoml.sklearn.get("iris_knn:latest").to_runner()
svc = bentoml.Service("iris_classifier", runners=[iris_clf_runner])
ROLLING_LOGGER_KEY_MODEL_1 = "model-1"

# Make sure to set WHYLABS_API_KEY in the environment before this is called
why.init()

@svc.on_startup
def startup(context: bentoml.Context) -> None:
    # One instance can log to any number of dataset
    logger = ProcessRollingLogger(
        aggregate_by=TimeGranularity.Hour,
        write_schedule=Schedule(cadence=TimeGranularity.Minute, interval=10),
    )

@svc.api(
    input=NumpyNdarray.from_sample(np.array([4.9, 3.0, 1.4, 0.2], dtype=np.double)),  # type: ignore
    output=Text(),
    route="v1/models/iris/predict",
)
def classify(features: NDArray[Any], context: bentoml.Context) -> str:
    results = iris_clf_runner.predict.run([features])
    probs = iris_clf_runner.predict_proba.run([features])
    result: int = results[0]

    category = CLASS_NAMES[result]
    prob = max(probs[0])

    # numpy values have to be converted to python types  because the process logger
    # requires serializable inputs
    data = {
        "sepal length": features[0].item(),
        "sepal width": features[1].item(),
        "petal length": features[2].item(),
        "petal width": features[3].item(),
        "class_output": category,
        "proba_output": prob.item(),
    }

    logger: ProcessRollingLogger = context.state[ROLLING_LOGGER_KEY]
    logger.log(data, dataset_id="model-1")

    return category
```

Running `bentoml serve service_process_logger:svc` will start the BentoML API server locally and log the model predictions and data profiles
to WhyLabs every 10 minutes, or when the server is shutdown.

- To learn how to deploy your BentoML model to production, check out the [BentoML documentation](https://docs.bentoml.org/en/latest/).
- To learn more about monitoring models in production, check out the [WhyLabs documentation](https://docs.whylabs.ai/docs/).

## Thread Logger vs Process Logger

There are two version of the server in this example: `server_thread_logger.py` and `server_process_logger.py`. In general, the process based
logger is better but it requires extra dependencies that only work on Mac/Linux that make the performance much better than the standard
multiprocessing options in Python. The thread based version has a few disadvantages worth mentioning here:

- It requires manually creating a logging instance for every dataset id you log to while the process based version handles that for you when
  you call with `logger.log(data, dataset_id="..")`.
- Python's GIL means that logging could contend with inference compute.
- It lacks optimizations that the process based logger has, like bulk data processing.

The process logger has some requirements to enable that boosted performance:

- Make sure to start the process logger early in the on_startup hook because it does use fork.
- If process forking isn't ok for some reason then you'll have to use the thread version.
- It requires extra dependencies installed through `pip install whylogs[proc]`.
- It requires manually calling the `.start()` method to start it after creation.
- It requires the things that you log are serializable, which means converting numpy types into python types by calling `.item()` on them.
- Requires Mac/Linux (or WSL). Won't work natively on Windows.
- Has native code and requires the ability to build it. For example, you would need to run `sudo apt install build-essential` on Ubuntu).
- It can actually run in parallel with your inference code when hosts have multiple cores.
- Is asynchronous by default, so make sure you leave logging enabled when you're setting things up so you can see errors in logs.

If you can use the process based logger then you should. If the thread based version appears to perform good enough and you want to make
sure you can run it in your local Windows development flow then the thread version is fine as well. Just make sure you look at each example
because there are some slight diffrences in their apis.

In both cases you should make sure to create the logger in the `on_startup` hook, as opposed to at the top level of your service because
BentoML is going to create several instances of server workers, potentially on separate machines, and prefers you respect its lifecycle
semantics.

## 🚀 How to run the example:

### 1. Install dependencies

```bash
pip install -r requirements.txt

# Install the proc extra as well if you're going to use the process based logger. This only suports Mac/Linux at the moment.
pip install whylogs[proc]
```

### 2. Train and save model

Train and save a version of the model with BentoML and scikit-learn.

```bash
python train.py
```

### 3. Update WhyLabs environment variables

In `service.py` update the [WhyLabs](https://whylabs.ai/) environment variables with your API key, org-id and project-id.

```python
os.environ['WHYLABS_API_KEY'] = 'APIKEY'
# Optional, dataset id can also go directly in source code. If you have multiple datasets then you'll need to include them in source.
os.environ["WHYLABS_DEFAULT_DATASET_ID"] = 'PROJECTID'
```

### 4. Serve the model with BentoML locally

Run the API server with BentoML to test the API locally.

```bash
bentoml serve server_thread_logger:svc
```

### 5. Test the API locally

With the server running, you can test the API with the Swagger UI, curl, or any HTTP client:

To test with Swagger UI, go to http://localhost:3000 and click on the `classify` endpoint.

To test the API with curl, run the following command:

```bash
curl --header "Content-Type: application/json" \
    --request POST \
    --data '[[4.9, 3.0, 1.4, 0.2]]' \
    http://localhost:3000/v1/models/iris/predict
```

Kill the server with ctrl-c to force the whylogs profiles to upload to WhyLabs.

### 6. View the logged data in WhyLabs & Setup Monitoring

In the WhyLabs project, you will see the data profile for the iris dataset and the model predictions being logged. You will need to wait the 5 minutes set in the rolling log before the data is written to WhyLabs.

You can enable ML monitoring for your model and data in the `monitor` section of the WhyLabs dashboard.

![WhyLabs](https://camo.githubusercontent.com/8e9cc18b64b157d4569fa6ed2bd5152200ee7bb1a11e54f858f923a4be635f90/68747470733a2f2f7768796c6162732e61692f5f6e6578742f696d6167653f75726c3d6874747073253341253246253246636f6e74656e742e7768796c6162732e6169253246636f6e74656e74253246696d616765732532463230323225324631312532464672616d652d363839392d2d312d2e706e6726773d3331323026713d3735)

See the [WhyLabs documentation](https://docs.whylabs.ai/docs/) or this [blog post](https://whylabs.ai/blog/posts/ml-monitoring-in-under-5-minutes) for more information on how to use the WhyLabs dashboard.

## Deployment with BentoML

To get your BentoML project ready for deployment, check out the [BentoML documentation](https://docs.bentoml.org/en/latest/).

Add `whylogs[proc]` under python packages in the `bentofile.yaml` file to use whylogs and WhyLabs in a production BentoML project. If you're
deploying on windows then just use `whylogs` without the proc extra (Thread Logger vs Process Logger section).

```yaml
python:
  packages: # Additional pip packages required by the service
    - whylogs[whylabs]
```

## 📚 More Resources

- [Sign up for WhyLabs](https://whylabs.ai/)
- [whylogs Documentation](https://whylogs.readthedocs.io/en/latest/)
- [WhyLabs Documentation](https://docs.whylabs.ai/docs/)
- [BentoML Documentation](https://docs.bentoml.org/en/latest/)
- [WhyLabs Blog](https://whylabs.ai/blog)
