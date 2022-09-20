import json
from optparse import Option
from typing import Any, Dict, Tuple, Type, TypedDict

from whylogs.api.writer import Writers
from whylogs.api.writer.writer import Optional, Writable, Writer


class FeatureWeightResponse(TypedDict):
    segmentWeights: list
    metadata: Dict


class FeatureWeightDict(TypedDict):
    segment: Optional[str]
    weights: Dict[str, float]


class FeatureWeight(Writable):
    def __init__(self, weights: Dict[str, float], segment: Optional[str] = None):
        """Feature Weights

        Parameters
        ----------
        weights : Dict[str, float]
            Feature weights
        segment : str, optional
            If segment is None, weights are considered to be for the complete unsegmented data, by default None
        """
        self.weights = weights
        self.segment = segment

    def writer(self, name: str = "local") -> "FeatureWeightWriter":
        writer = Writers.get(name)
        return FeatureWeightWriter(feature_weight=self, writer=writer)

    def get_default_path(self) -> str:
        pass

    def write(self, path: Optional[str] = None, **kwargs: Any) -> None:
        pass

    def to_json(self) -> str:
        return json.dumps({"segment": self.segment, "weights": self.weights})

    def to_dict(self) -> FeatureWeightDict:
        return {"segment": self.segment, "weights": self.weights}


class FeatureWeightWriter(object):
    def __init__(self, feature_weight, writer: Writer) -> None:
        self._feature_weight = feature_weight
        self._writer = writer

    def option(self, **kwargs) -> "FeatureWeightWriter":
        self._writer.option(**kwargs)
        return self

    def write(self, **kwargs: Any) -> Tuple[bool, str]:
        """Put feature weights for the specified dataset.

        Returns
        -------
        Tuple[bool, str]
            Tuple with a boolean (1-success, 0-fail) and string with the PUT request's status code.
        """
        return self._writer.write(file=self._feature_weight, **kwargs)

    def get_feature_weights(self, **kwargs: Any) -> FeatureWeightResponse:
        """Get latest version for the feature weights for the specified dataset

        Returns
        -------
        FeatureWeightResponse
            Response of the GET request, with segmentWeights and metadata.
        """
        return self._writer.get_feature_weights(**kwargs)
