from typing import List

from sklearn.utils.multiclass import type_of_target
import numpy as np


from whylogs.proto import ModelProfileMessage
from whylogs.core.metrics.model_metrics import ModelMetrics

SUPPORTED_TYPES = ("binary", "multiclass")


class ModelProfile:

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
                 output_fields: List[str] = [],
                 metrics: ModelMetrics = None):
        super().__init__()

        self.output_fields = output_fields
        if metrics:
            self.metrics = metrics
        else:
            self.metrics = ModelMetrics()

    # def compute_full_metrics(self,):

    def add_output_field(self, field: str):
        if field not in self.output_fields:
            self.output_fields.append(field)

    def compute_metrics(self, targets,
                        predictions,
                        scores=None,
                        target_field=None,
                        prediction_field=None,
                        score_field=None
                        ):
        """
        Compute and track metrics for confusion_matrix

        Parameters
        ----------
        targets : List
            targets (or actuals) for validation
        predictions : List
            predictions (or inferred values)
        scores : List, optional
            associated scores for each prediction
        target_field : str, optional
        prediction_field : str, optional
        score_field : str, optional


        Raises
        ------
        NotImplementedError

        """
        tgt_type = type_of_target(targets)
        if tgt_type not in ("binary", "multiclass"):
            raise NotImplementedError("target type not supported yet")
        # if score are not present set them to 1.
        if scores is None:
            scores = np.ones(len(targets))

        scores = np.array(scores)

        # compute confusion_matrix
        self.metrics.compute_confusion_matrix(predictions=predictions,
                                              targets=targets,
                                              scores=scores,
                                              target_field=target_field,
                                              prediction_field=prediction_field,
                                              score_field=score_field)

    def to_protobuf(self):

        return ModelProfileMessage(output_fields=self.output_fields,
                                   metrics=self.metrics.to_protobuf()
                                   )

    @classmethod
    def from_protobuf(self, message):

        return ModelProfile(output_fields=message.output_fields,
                            metrics=ModelMetrics.from_protobuf(message.metrics))

    def merge(self, model_profile):
        output_fields = list(
            set(self.output_fields+model_profile.output_fields))
        metrics = self.metrics.merge(model_profile.metrics)
        return ModelProfile(output_fields=output_fields, metrics=metrics)
