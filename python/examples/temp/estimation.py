from whylogs.datasets import Ecommerce
from whylogs.experimental.performance_estimation.estimators import AccuracyEstimator

dataset = Ecommerce()

baseline = dataset.get_baseline()

import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn’

import os
from datetime import datetime, timedelta, timezone

os.environ["WHYLABS_API_ENDPOINT"] = "https://songbird.development.whylabsdev.com"
# set your org-id here - should be something like "org-xxxx"
print("Enter your WhyLabs Org ID")
os.environ["WHYLABS_DEFAULT_ORG_ID"] = "org-HVB9AM"

# set your datased_id (or model_id) here - should be something like "model-xxxx"
print("Enter your WhyLabs Dataset ID")
os.environ["WHYLABS_DEFAULT_DATASET_ID"] = "model-36"


# set your API key here
print("Enter your WhyLabs API key")
os.environ["WHYLABS_API_KEY"] = "z8fYdnQwHr.ibJaqDpZSsZd9dpo5ILyKlOgwXWPV7LGvtIsyqFUs54MGUsHMNz6q"
print("Using API Key ID: ", os.environ["WHYLABS_API_KEY"][0:10])


def arrange_df(batch):
    df = batch.features
    df["output_discount"] = batch.target["output_discount"]
    df["output_prediction"] = batch.prediction["output_prediction"]
    return df


reference_df = arrange_df(baseline)
reference_df.head()

batch = dataset.get_inference_data(number_batches=1)

unperturbed_df = arrange_df(next(iter(batch)))

import whylogs as why
from whylogs.core.segmentation_partition import segment_on_column
from whylogs.core.schema import DatasetSchema


def log_dataset(df, labeled=True):
    segment_column = "category"
    segmented_schema = DatasetSchema(segments=segment_on_column(segment_column))

    # Just to be sure that we're not using actual labels/metrics for the target dataset.
    if labeled:
        results = why.log_classification_metrics(
            df,
            target_column="output_discount",
            prediction_column="output_prediction",
            schema=segmented_schema,
            log_full_data=True,
        )
        return results
    else:
        results = why.log(df, schema=segmented_schema)
        return results


reference_results = log_dataset(reference_df, labeled=True)
perturbed_results = log_dataset(unperturbed_df, labeled=False)

estimator = AccuracyEstimator(reference_result_set=reference_results)

estimation_result = estimator.estimate(perturbed_results)

print(perturbed_results._performance_estimation)

perturbed_results.writer("whylabs").write()

for day in range(7):
    dataset_timestamp = datetime.now() - timedelta(days=day)
    dataset_timestamp = dataset_timestamp.replace(tzinfo=timezone.utc)
    perturbed_results.set_dataset_timestamp(dataset_timestamp)
    estimation_result = estimator.estimate(perturbed_results)
    perturbed_results.writer("whylabs").write()
