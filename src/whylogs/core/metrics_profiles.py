from typing import List

from sklearn.utils.multiclass import type_of_target
import numpy as np
import pandas as pd

from whylogs.proto import ModelSummary

from whylogs.core.metrics.confusion_matrix import ConfusionMatrix
from whylogs.core import ColumnProfile
from whylogs.core.metrics.metrics import Metrics

SUPPORTED_TYPES = ("binary", "multiclass")


class Model:

    """
    Model Class for sketch metrics for model outputs

    Attributes
    ----------
    confusion_matrix : ConfusionMatrix
        Confusion Matrix object
    labels : list
        list of label associated with classification
    name : str
        model name
    profiles : dict
        a dictonary of column profiles to sketch metrics
    """

    def __init__(self,
                 output_fields: List[str] = None,
                 metrics: Metrics = None):
        super().__init__()

        self.output_fields = output_fields
        self.metrics = metrics

    # def compute_full_metrics(self,):

    def add_output_field(self, field: str):
        if field not in self.output_fields:
            self.output_fields.append(field)

    def compute_metrics(self, targets,
                        predictions,
                        scores=None,
                        target_field_name=None,
                        prediction_field_name=None,
                        score_field_name=None
                        ):

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

        # compute confusion_matrix
        self.compute_confusion_matrix(uniques, targets, predictions)

    def compute_confusion_matrix(self, labels, targets, predictions):

        confusion_matrix = ConfusionMatrix(labels)
        confusion_matrix.add(targets, predictions)

        if self.confusion_matrix is None:
            self.confusion_matrix = confusion_matrix
        else:
            self.confusion_matrix = self.confusion_matrix.merge(
                confusion_matrix)

    def to_protobuf(self):

        return ModelSummary(

            confusion_matrix=self.confusion_matrix.to_protobuf(
            ) if self.confusion_matrix else None,

        )

    def from_protobuf(self, message):

        confusion_Matrix = ConfusionMatrix.from_protobuf(
            message.model.confusion_matrix)

        mod = Model(message.name, labels=message.labels, metrics={
            k: ColumnProfile.from_protobuf(v) for k, v in message.metrics.items()
        })
        mod.confusion_matrix = confusion_Matrix
        return mod

    # def calculate_metric(self, label, n_bins=10):

    #     bins = np.array(range(0, n_bins))/n_bins
    #     # print(bins)
    #     tp_profile = self.profiles["whylogs.metrics.true_positive_scores.{}".format(
    #         label)].number_tracker

    #     fp_profile = self.profiles["whylogs.metrics.false_positive_scores.{}".format(
    #         label)].number_tracker

    #     fn_profile = self.profiles["whylogs.metrics.false_negative_scores.{}".format(
    #         label)].number_tracker

    #     tn_profile = self.profiles["whylogs.metrics.true_negative_scores.{}".format(
    #         label)].number_tracker

    #     if tp_profile.floats.count > 0:
    #         tp_cum = np.array(tp_profile.histogram.get_cdf(
    #             bins))*tp_profile.floats.count
    #     else:
    #         tp_cum = np.append(np.zeros_like(bins), 0.0)

    #     if fp_profile.floats.count > 0:
    #         fp_cum = np.array(fp_profile.histogram.get_cdf(
    #             bins))*fp_profile.floats.count
    #     else:
    #         fp_cum = np.append(np.zeros_like(bins), 0.0)

    #     if tn_profile.floats.count > 0:
    #         tn_cum = np.array(
    #             tn_profile.histogram.get_cdf(bins))*tn_profile.floats.count
    #     else:
    #         tn_cum = np.append(np.zeros_like(bins), 0.0)

    #     if fn_profile.floats.count > 0:
    #         fn_cum = np.array(fn_profile.histogram.get_cdf(
    #             bins))*fn_profile.floats.count
    #     else:
    #         fn_cum = np.append(np.zeros_like(bins), 0.0)

    #     min_val = np.finfo(float).eps
    #     recall = tp_cum/(np.add(tp_cum, fn_cum) + min_val)
    #     precision = tp_cum/(np.add(tp_cum, fp_cum) + min_val)
    #     # fpr = fp_cum / (np.add(fp_cum, tn_cum) + min_val)
    #     return bins, recall, precision

    # def calculate_roc(self, label, n_bins=10):

    #     bins = np.array(range(0, n_bins))/n_bins
    #     # print(bins)
    #     tp_profile = self.profiles["whylogs.metrics.true_positive_scores.{}".format(
    #         label)].number_tracker

    #     fp_profile = self.profiles["whylogs.metrics.false_positive_scores.{}".format(
    #         label)].number_tracker

    #     fn_profile = self.profiles["whylogs.metrics.false_negative_scores.{}".format(
    #         label)].number_tracker

    #     tn_profile = self.profiles["whylogs.metrics.true_negative_scores.{}".format(
    #         label)].number_tracker

    #     if tp_profile.floats.count > 0:
    #         tp_cum = np.array(tp_profile.histogram.get_cdf(
    #             bins))*tp_profile.floats.count
    #     else:
    #         tp_cum = np.append(np.zeros_like(bins), 0.0)

    #     if fp_profile.floats.count > 0:
    #         fp_cum = np.array(fp_profile.histogram.get_cdf(
    #             bins))*fp_profile.floats.count
    #     else:
    #         fp_cum = np.append(np.zeros_like(bins), 0.0)

    #     if tn_profile.floats.count > 0:
    #         tn_cum = np.array(
    #             tn_profile.histogram.get_cdf(bins))*tn_profile.floats.count
    #     else:
    #         tn_cum = np.append(np.zeros_like(bins), 0.0)

    #     if fn_profile.floats.count > 0:
    #         fn_cum = np.array(fn_profile.histogram.get_cdf(
    #             bins))*fn_profile.floats.count
    #     else:
    #         fn_cum = np.append(np.zeros_like(bins), 0.0)

    #     min_val = np.finfo(float).eps
    #     recall = tp_cum/(np.add(tp_cum, fn_cum) + min_val)
    #     fpr = fp_cum / (np.add(fp_cum, tn_cum) + min_val)
    #     return bins, recall[1::], fpr[1::]

    # def calculate_f_score(self, label, n_bins=10, beta=1):
    #     # Calculates
    #     bins, recall, precision = self.calculate_metrics(label, n_bins)
    #     f_score = []
    #     for indx in range(len(recall[1:])):
    #         f_score.append(f_score_beta(
    #             beta=beta, precision=precision[indx], recall=recall[indx]))
    #     return bins, f_score

    def merge(self, other_model):
        labels = []
        if self.labels:
            for lbl in self.labels:
                if lbl not in labels:
                    labels.append(lbl)
        if other.labels:
            for lbl in other_model.labels:
                if lbl not in labels:
                    labels.append(lbl)

        metrics_set = set(list(self.profiles.keys()) +
                          list(other.profiles.keys()))

        metrics = {}
        for col_name in metrics_set:
            empty_column = ColumnProfile(col_name)
            this_column = self.profiles.get(col_name, empty_column)
            other_column = other.profiles.get(col_name, empty_column)
            metrics[col_name] = this_column.merge(other_column)

        mod = Model(name=name, labels=label, metrics=metrics)

        mod.confusion_matrix = self.confusion_matrix.merge(
            other_model.confusion_matrix)
        return mod


def f_score_beta(beta, precision, recall):
    beta2 = beta*beta
    min_val = np.finfo(float).eps
    f_score = (1+beta2) + precision*recall/(beta2*precision+recall+min_val)
    return f_score


def enconde_to_integers(values, uniques):
    table = {val: i for i, val in enumerate(uniques)}
    return np.array([table[v] for v in values])
