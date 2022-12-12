import numpy as np
import pandas as pd
import seaborn as sns

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

import datetime
from typing import List
import whylogs as why
from whylogs.core.model_performance_metrics.model_performance_metrics import (
    ModelPerformanceMetrics)
from whylogs.core.schema import DatasetSchema
from whylogs.core.segmentation_partition import segment_on_column

def split_data(data, segment_columns, continuous_numerical_columns, discrete_numerical_columns,
               categorical_columns, text_columns, target_column, prediction_column, score_column,
               split_batches, split_vol_escalation_frac, split_vol_noise_frac):
# Label correct and incorrect predictions
    data["is_correct"] = data.apply(lambda row: row[target_column] == row[prediction_column], axis=1)

    for col_name in segment_columns:
        # choose one to drop in accuracy
        np.random.choice(data["category"].unique())

    split_vol_probs = ((split_vol_escalation_frac * np.arange(1, split_batches + 1) / 
                    (split_batches * (split_batches + 1) / 2) + 
                    (1-split_vol_escalation_frac)/split_batches))
    split_vol_adj = np.random.uniform(-split_vol_noise_frac, 
                                split_vol_noise_frac, 
                                split_batches-1)
    split_vol_adj = np.append(split_vol_adj, 1 - split_vol_adj)

    # Split by volume
    data["day"] = np.random.choice(a=split_batches,
                                size=len(data), 
                                p=split_vol_probs)
    
    for seg_col in segment_columns:
        data = pd.concat([data,
            data[(data["day"] == np.random.randint(6)) & (data["is_correct"] == True) & (data[seg_col] == np.random.choice(data[seg_col].unique()))].sample(frac=np.random.uniform(0.25, 0.5)),
            data[(data["day"] == np.random.randint(6)) & (data["is_correct"] == False) & (data[seg_col] == np.random.choice(data[seg_col].unique()))].sample(frac=np.random.uniform(0.25, 0.5))]
        )
    
    return data

def lookup_typed_key(grouped_pdata, key: str):
    typed_keys = set(grouped_pdata.indices.keys())
    for typed_key in typed_keys:
        if key == str(typed_key):
            return typed_key
    return key


#create a helper method to add performance metrics to segmented pandas dataframes
def add_performance_to_segments(grouped_pdf, p_column, t_column, s_column, results):
    partition = results.partitions[0]
    segments = results.segments_in_partition(partition)
    for segment in segments:
        # A segment's key is a tuple of the column values, since we segmented on a single column
        # the first value in the tuple will be the same as a groupby key in pandas ('Baby care',),
        # so get that the pandas group using this key. e.g. "Baby care" etc
        typed_key = lookup_typed_key(grouped_pdf, segment.key[0])

        segmented_pdf = grouped_pdf.get_group(typed_key)
        perf = ModelPerformanceMetrics()
        labels = segmented_pdf[p_column].to_list()
        labels.extend(segmented_pdf[t_column].to_list())
        perf.compute_confusion_matrix(
            predictions=segmented_pdf[p_column].to_list(),
            targets=segmented_pdf[t_column].to_list(),
            scores=segmented_pdf[s_column].to_list()
        )
        segmented_profile = results.profile(segment)
        segmented_profile.add_model_performance_metrics(perf)


# helper method to profile and upload segmented perf metrics for each column
# (non-cartesian product) for the last n days
def upload_columnar_segmented_performance_for_past_n_days(
    data,
    segment_columns: List[str],
    prediction_column: str,
    target_column: str,
    score_column: str,
    n: int = 7):

    result_profiles = []
    for column_name in segment_columns:
        print(f"** Profiling segmented performance metrics for column({column_name}) "
              f"using targets ({target_column}) and predictions ({prediction_column})")
        
        temp_df = data.copy()
        temp_df["temp_target"] = temp_df[target_column]
        temp_df["temp_prediction"] = temp_df[prediction_column]

        # split into binary classification when segmenting on performance classes
        if column_name in [target_column, prediction_column]:
            for label, label_idx in df.groupby(column_name).groups.items():
                temp_df.loc[label_idx, "temp_target"] = temp_df.loc[label_idx, "temp_target"].apply(
                    lambda x, label: 1 if x == label else 0, 
                    label=label
                )
                temp_df.loc[label_idx, "temp_prediction"] = temp_df.loc[label_idx, "temp_prediction"].apply(
                    lambda x, label: 1 if x == label else 0, 
                    label=label
                )
                #print("Predictions:", label, temp_df.loc[label_idx, "temp_prediction"].value_counts())
                #print("Targets:", label, temp_df.loc[label_idx, "temp_target"].value_counts())

        # split the pandas data frame on the same column that segmented profiles used
        for i in range(n):
            df = temp_df[temp_df["day"] == i]
            grouped_pdf = df.groupby(column_name)

            dt = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=i)
            print(f"    About to log data for {i} days ago-> {dt}")
            results = why.log(df, schema=DatasetSchema(segments=segment_on_column(column_name)))
            print(f"    Segmented profile for {column_name} on {dt} has {results.count} segments")
            add_performance_to_segments(grouped_pdf, "temp_prediction", "temp_target", score_column, results)
            results.set_dataset_timestamp(dt)
            print(f"    Uploading profiles for {column_name} on {dt}, this may take a while...")
            results.writer("whylabs").write()
            print(f"    Done uploading profiles for {column_name} on {dt}")
            result_profiles.append(results)
    print(f"** Done uploading profiles!")
    return result_profiles