
import logging
logging.basicConfig(level=logging.DEBUG)
import random
import time

import pandas as pd
import mlflow
import whylogs

from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet
from whylogs import get_or_create_session
from dotenv import load_dotenv
load_dotenv()
assert whylogs.__version__ >= "0.1.13" # we need 0.1.13 or later for MLflow integration


whylogs.enable_mlflow()

data_url = "http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv"
data = pd.read_csv(data_url, sep=";")


# Split the data into training and test sets
train, test = train_test_split(data)



#session = get_or_create_session(path_to_config=".whylabs.yaml")
#summary = session.profile_dataframe(train, "training-data").flat_summary()['summary']


# Relocate predicted variable "quality" to y vectors
train_x = train.drop(["quality"], axis=1).reset_index(drop=True)
test_x = test.drop(["quality"], axis=1).reset_index(drop=True)
train_y = train[["quality"]].reset_index(drop=True)
test_y = test[["quality"]].reset_index(drop=True)

subset_test_x = []
subset_test_y = []
num_batches = 20
for i in range(num_batches):
    indices = random.sample(range(len(test)), 5)
    subset_test_x.append(test_x.loc[indices, :])
    subset_test_y.append(test_y.loc[indices, :])


# Create an MLflow experiment for our demo
experiment_name = "whylogs demo"
mlflow.set_experiment(experiment_name)

model_params = {"alpha": 1.0,
                "l1_ratio": 0.7}

lr = ElasticNet(**model_params)
lr.fit(train_x, train_y)
print("ElasticNet model (%s):" % model_params)

# run predictions on the batches of data we set up earlier and log whylogs data
for i in range(num_batches):
    with mlflow.start_run(run_name=f"Run {i + 1}"):
        batch = subset_test_x[i]
        predicted_output = lr.predict(batch)

        mae = mean_absolute_error(subset_test_y[i], predicted_output)
        print("Subset %.0f, mean absolute error: %s" % (i + 1, mae))

        mlflow.log_params(model_params)
        mlflow.log_metric("mae", mae)

        # use whylogs to log data quality metrics for the current batch
        mlflow.whylogs.log_pandas(batch)

    # wait a second between runs to create a time series of prediction results
    time.sleep(1)
