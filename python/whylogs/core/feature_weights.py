import json
from typing import Any, Dict, List, Optional, Tuple, Union

from whylogs.api.writer.writer import WriterWrapper, _Writable
from whylogs.core import Segment
from whylogs.core.utils import deprecated


class FeatureWeights(_Writable):
    def __init__(self, weights: Dict[str, float], segment: Optional[Segment] = None, metadata: Optional[Dict] = None):
        """Feature Weights

        Parameters
        ----------
        weights : Dict[str, float]
            Feature weights
        segment : str, optional
            If segment is None, weights are considered to be for the complete unsegmented data, by default None
        """
        self.weights = weights
        if segment:
            raise NotImplementedError("Segmented Feature Weights is currently not supported.")
        self.segment = segment
        self.metadata = metadata

    def _get_default_filename(self) -> str:
        raise ValueError("I'm not a real Writable")

    def _get_default_path(self) -> str:
        raise ValueError("I'm not a real Writable")

    @deprecated(message="please use a Writer")
    def write(self, path: Optional[str] = None, **kwargs: Any) -> Tuple[bool, str]:
        raise ValueError("I'm not a real Writable")

    def _write(self, path: Optional[str] = None, **kwargs: Any) -> Tuple[bool, Union[str, List[str]]]:
        raise ValueError("I'm not a real Writable")

    def to_json(self) -> str:
        return json.dumps({"segment": self.segment, "weights": self.weights})

    def to_dict(self) -> Dict[str, Union[Optional[Segment], Optional[float]]]:
        return {"segment": self.segment, "weights": self.weights}


FeatureWeightWriter = WriterWrapper
