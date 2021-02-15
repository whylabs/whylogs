from typing import List
import numbers
from sklearn.utils.multiclass import type_of_target
from whylogs.core import ColumnProfile
import numpy as np
import pandas as pd

SUPPORTED_TYPES = ("binary", "multiclass")


class Profile:

    def __init__(self, profiles: dict = {}):
        self.profiles = profiles

    def track(self, columns, data=None):
        """
        Add value(s) to tracking statistics for column(s)

        Parameters
        ----------
        columns : str, dict
            Either the name of a column, or a dictionary specifying column
            names and the data (value) for each column
            If a string, `data` must be supplied.  Otherwise, `data` is
            ignored.
        data : object, None
            Value to track.  Specify if `columns` is a string.
        """
        if data is not None:
            if type(columns) != str:
                raise TypeError("Unambigious column to data mapping")
            self.track_datum(columns, data)
        else:
            if isinstance(columns, dict):
                for column_name, data in columns.items():
                    self.track_datum(column_name, data)
            elif isinstance(columns, str):
                self.track_datum(columns, None)
            else:
                raise TypeError(" Data type of: {} not supported for tracking ".format(
                    columns.__class__.__name__))

    def track_datum(self, column_name, data):
        try:
            prof = self.profiles[column_name]
        except KeyError:
            prof = ColumnProfile(column_name)
            self.profiles[column_name] = prof

        prof.track(data)

    def track_array(self, x: np.ndarray, columns=None):
        """
        Track statistics for a numpy array

        Parameters
        ----------
        x : np.ndarray
            2D array to track.
        columns : list
            Optional column labels
        """
        x = np.asanyarray(x)
        if np.ndim(x) != 2:
            raise ValueError("Expected 2 dimensional array")
        if columns is None:
            columns = np.arange(x.shape[1])
        columns = [str(c) for c in columns]
        return self.track_dataframe(pd.DataFrame(x, columns=columns))

    def track_dataframe(self, df: pd.DataFrame):
        """
        Track statistics for a dataframe

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to track
        """
        # workaround for CUDF due to https://github.com/rapidsai/cudf/issues/6743
        # if cudfDataFrame is not None and isinstance(df, cudfDataFrame):
        #     df = df.to_pandas()
        for col in df.columns:
            col_str = str(col)
            x = df[col].values
            for xi in x:
                self.track(col_str, xi)


class Model(Profile):

    def __init__(self, name, labels: List[str] = None):
        super().__init__()

        self.name = name
        self.profiles = {}
        self.confusion_matrix = None
        if labels:
            self.labels = list(np.unique(labels))
            num_labels = len(self.labels)
            self.confusion_matrix = np.zeros((num_labels, num_labels))
        else:
            self.labels = None

    # def compute_full_metrics(self,):

    def track_metrics(self, targets, predictions, scores=None):

        for each_target in targets:
            self.track("targets", each_target)
        for each_pred in predictions:
            self.track("predictions", each_pred)
        if scores:
            for each_score in scores:
                self.track("scores", each_score)

        tgt_type = type_of_target(targets)
        if tgt_type not in ("binary", "multiclass"):
            raise NotImplementedError("target type not supported yet")
        # if score are not present set them to 1.
        if scores is None:
            scores = np.ones(len(targets))

        scores = np.array(scores)
        if self.labels is None:
            self.labels = []

        # get unique labels in targets and predictions
        for targt in targets:
            if targt not in self.labels:
                self.labels.append(targt)

        for pred in predictions:
            if pred not in self.labels:
                self.labels.append(pred)

        uniques = self.labels

        encoded_targets = enconde_to_integers(targets, uniques)
        encoded_predictions = enconde_to_integers(predictions, uniques)

        # compute confusion_matrix
        for each_tar in encoded_targets:
            for each_pred in encoded_predictions:
                self.confusion_matrix[each_tar, each_pred] += 1

        for each_enco_target in enconde_to_integers(uniques, uniques):

            mask = (encoded_targets == each_enco_target)
            mask_pred = (encoded_predictions == each_enco_target)

            tp_scores = scores[mask & mask_pred]
            tp_scores_name = "whylogs.metrics.true_positive_scores.{}".format(
                uniques[each_enco_target])

            fp_scores = scores[~mask & mask_pred]
            fp_scores_name = "whylogs.metrics.false_positive_scores.{}".format(
                uniques[each_enco_target])

            tn_scores = scores[~mask & ~mask_pred]
            tn_scores_name = "whylogs.metrics.true_negative_scores.{}".format(
                uniques[each_enco_target])

            fn_scores = scores[mask & ~mask_pred]
            fn_scores_name = "whylogs.metrics.false_negative_scores.{}".format(
                uniques[each_enco_target])

            prof = ColumnProfile(tp_scores_name)
            self.profiles[tp_scores_name] = prof
            if len(tp_scores) > 0:
                self.track_array(columns=[tp_scores_name],
                                 x=tp_scores.reshape((-1, 1)))

            prof = ColumnProfile(fp_scores_name)
            self.profiles[fp_scores_name] = prof
            if len(fp_scores) > 0:
                self.track_array(columns=[fp_scores_name],
                                 x=fp_scores.reshape((-1, 1)))
            prof = ColumnProfile(tn_scores_name)
            self.profiles[tn_scores_name] = prof
            if len(tn_scores) > 0:
                self.track_array(columns=[tn_scores_name],
                                 x=tn_scores.reshape((-1, 1)))
            prof = ColumnProfile(fn_scores_name)
            self.profiles[fn_scores_name] = prof
            if len(fn_scores) > 0:
                self.track_array(columns=[fn_scores_name],
                                 x=fn_scores.reshape((-1, 1)))


def enconde_to_integers(values, uniques):
    table = {val: i for i, val in enumerate(uniques)}
    return np.array([table[v] for v in values])
