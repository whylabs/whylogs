from logging import getLogger
from typing import Dict, List, Optional, Set, Union

from whylogs.core.metrics.metrics import Metric
from whylogs.core.proto.v0 import ModelMetricsMessage, ModelProfileMessage, ModelType

from .confusion_matrix import ConfusionMatrix
from .regression_metrics import RegressionMetrics

OUTPUT_FIELD_CATEGORY = "output"
logger = getLogger(__name__)


class ModelPerformanceMetrics:
    """
    Container class for various model-related performance metrics

    Attributes:
        confusion_matrix (ConfusionMatrix): ConfusionMatrix which keeps it track of counts with NumberTracker
        regression_metrics (RegressionMetrics): Regression Metrics keeps track of a common regression metrics in case the targets are continous.
    """

    def __init__(
        self,
        confusion_matrix: Optional[ConfusionMatrix] = None,
        regression_metrics: Optional[RegressionMetrics] = None,
        metrics: Optional[Dict[str, Metric]] = None,
        field_metadata: Optional[Dict[str, Set[str]]] = None,
    ):
        self.model_type = None
        self.confusion_matrix = confusion_matrix
        self.regression_metrics = regression_metrics
        self.metrics = metrics
        self.field_metadata = field_metadata

    def to_protobuf(
        self,
    ) -> ModelProfileMessage:
        model_type = ModelType.UNKNOWN
        if not self.model_type:
            if self.confusion_matrix:
                model_type = ModelType.CLASSIFICATION
            elif self.regression_metrics:
                model_type = ModelType.REGRESSION
        else:
            model_type = self.model_type

        model_metrics = ModelMetricsMessage(
            scoreMatrix=self.confusion_matrix.to_protobuf() if self.confusion_matrix else None,
            regressionMetrics=self.regression_metrics.to_protobuf() if self.regression_metrics else None,
            modelType=model_type,
        )

        return ModelProfileMessage(output_fields=self.output_fields, metrics=model_metrics)

    @classmethod
    def from_protobuf(
        cls,
        message: ModelProfileMessage,
    ) -> "ModelPerformanceMetrics":
        # TODO: update format to support storing other field metadata, for now just support output field in v0 message format
        output_fields = (
            {column_name: set([OUTPUT_FIELD_CATEGORY]) for column_name in message.output_fields}
            if message.output_fields
            else None
        )
        confusion_matrix = None
        regression_metrics = None
        if message.metrics is None:
            logger.warning("deserializing a ModelPerformanceMetrics without a metrics field")
        else:
            confusion_matrix = ConfusionMatrix.from_protobuf(message.metrics.scoreMatrix)
            regression_metrics = RegressionMetrics.from_protobuf(message.metrics.regressionMetrics)

        return ModelPerformanceMetrics(
            confusion_matrix=confusion_matrix, regression_metrics=regression_metrics, field_metadata=output_fields
        )

    def compute_confusion_matrix(
        self,
        predictions: List[Union[str, int, bool, float]],
        targets: List[Union[str, int, bool, float]],
        scores: Optional[List[float]] = None,
    ):
        """
        computes the confusion_matrix, if one is already present merges to old one.

        Args:
            predictions (List[Union[str, int, bool]]):
            targets (List[Union[str, int, bool]]):
            scores (List[float], optional):
        """
        labels = sorted(list(set(targets + predictions)))
        confusion_matrix = ConfusionMatrix(labels=labels)
        confusion_matrix.add(predictions, targets, scores)

        if self.confusion_matrix is None or self.confusion_matrix.labels is None or self.confusion_matrix.labels == []:
            self.confusion_matrix = confusion_matrix
        else:
            self.confusion_matrix = self.confusion_matrix.merge(confusion_matrix)

    def compute_regression_metrics(
        self,
        predictions: List[Union[float, int]],
        targets: List[Union[float, int]],
    ):
        regression_metrics = RegressionMetrics()
        regression_metrics.add(predictions, targets)
        if self.regression_metrics:
            self.regression_metrics = self.regression_metrics.merge(regression_metrics)
        else:
            self.regression_metrics = regression_metrics

    def add_metadata_to_field(self, column_name: str, categories: Set[str]) -> None:
        if not self.field_metadata:
            self.field_metadata = dict()
        self.field_metadata[column_name] = categories

    def _set_column_value(self, column_name, value):
        if column_name not in self.field_metadata:
            self.field_metadata[column_name] = set([value])
        else:
            self.field_metadata[column_name].add(value)

    def specify_output_fields(self, column_names: Union[str, Set[str]]) -> None:
        if not self.field_metadata:
            self.field_metadata = dict()
        if isinstance(column_names, str):
            self._set_column_value(column_names, OUTPUT_FIELD_CATEGORY)
        else:
            for column_name in column_names:
                self._set_column_value(column_name, OUTPUT_FIELD_CATEGORY)

    @property
    def output_fields(self) -> Optional[List[str]]:
        output_column_names = None
        if self.field_metadata:
            for column_name in self.field_metadata:
                field_categories = self.field_metadata[column_name]
                if OUTPUT_FIELD_CATEGORY in field_categories:
                    if output_column_names is None:
                        output_column_names = []
                    output_column_names.append(column_name)
        return output_column_names

    def merge(self, other) -> "ModelPerformanceMetrics":
        """
        :type other: ModelMetrics
        """
        if other is None or (other.confusion_matrix is None and other.regression_metrics is None):
            return self

        merged_field_metadata: Optional[Dict[str, Set[str]]] = None
        if self.field_metadata is not None:
            if other.field_metadata is None:
                merged_field_metadata = self.field_metadata
            else:
                merged_keys = set(self.field_metadata.keys() + other.field_metadata.keys)
                merged_field_metadata = dict()
                for key in merged_keys:
                    categories = self.field_metadata.get(key)
                    other_categories = other.field_metadata.get(key)
                    if categories and other_categories:
                        merged_field_metadata[key] = categories.union(other_categories)
                    elif categories:
                        merged_field_metadata[key] = categories
                    else:
                        merged_field_metadata[key] = other_categories

        else:
            merged_field_metadata = other.field_metadata
        self.field_metadata = merged_field_metadata
        return ModelPerformanceMetrics(
            confusion_matrix=self.confusion_matrix.merge(other.confusion_matrix)
            if self.confusion_matrix
            else other.confusion_matrix,
            regression_metrics=self.regression_metrics.merge(other.regression_metrics)
            if self.regression_metrics
            else other.regression_metrics,
            field_metadata=merged_field_metadata,
        )
