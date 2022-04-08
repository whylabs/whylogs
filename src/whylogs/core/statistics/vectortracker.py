from io import BytesIO

import numpy as np

from whylogs.proto import VectorsMessage


_SMALL = np.finfo(float).eps

def _reciprocal(s: np.ndarray) -> np.ndarray:
    """ Return pseudoinverse of singular value vector """
    # should also zap if too small relative to s[0]
    return np.array([1/x if x > _SMALL else 0 for x in s])


def _serialize_ndarray(a: np.ndarray) -> bytes:
    bio = BytesIO()
    np.save(bio, a, allow_pickle=False)


def _deserialzie_ndarray(a: bytes) -> np.ndarray:
    bio = BytesIO(a)
    return np.load(bio, allow_pickle=False)


class VectorTracker:
    """
    Prototype SVD tracker
    """

    def __init__(self, rows: int, k: int, decay: float=1.0, u: Optional[np.ndarray]=None, s: Optional[np.ndarray]=None) -> None:
        assert rows > 1
        assert 0.0 < decay <= 1.0
        assert k > 1
        # assert compatible if other is not None
        # to cope with open vocabularies, we need to zero pad the _sketch
        #   np.pad(self._sketch, ((0, <new vocab size>), (0,0)), 'constant')

        self._rows = rows
        self._decay = decay
        self._k = k
        self._U = u.copy() if u else np.zeros((rows, k))
        self._S = s.copy() if s else np.zeros(k)

    def _resketch(self, U1: np.ndarray, S1: np.ndarray) -> None:
        U0, S0 = self._U, self._S
        Q, R = np.linalg.qr(np.concatenate((self._decay * U0 * S0, U1 * S1), axis=1))
        UR, self._S, VRT = np.linalg.svd(R)
        self._U = np.dot(Q, UR)

    def similarity(self, vector: np.ndarray) -> float:
        # similarity = 1 - || U S S^+ U' vector - vector || / || vector ||
        # S S^+ could probably just be I with the appropriate singular values zeroed
        similarity = self._U.transpose * vector
        similarity = _reciprocal(self._S) * similarity
        similarity = self._S * similarity
        similarity = self._U * similarity
        similarity = similarity - vector
        similarity = 1.0 - np.linalg.norm(similarity) / np.linalg.norm(vector)
        return similarity
        
    def track(self, vector: np.ndarray) -> None:
        assert vector.shape[0] == self._rows # need to reshape for open vocabularies
        U1, S1, _ = np.linalg.svd(vector.reshape((self._rows, 1)), False, True, False)
        self._resketch(U1, S1)

    def track_and_sim(self, vector: np.ndarray) -> float:
        self.track(vector)
        return similarity(vector)

    def merge(self, other):
        # assert compatible
        new_tracker = VectorTracker(self._rows, self._k, self._decay, self._U, self._S)
        new_tracker._resketch(other._U, other._S)
        return new_tracker

    def to_protobuf(self) -> VectorsMessage:
        return VectorsMessage(
            rows = self._rows,
            truncation = self._k,
            decay = self._decay,
            U = _serialize_ndarray(self._U)
            S = _serialize_ndarray(self._S)
        )

    @staticmethod
    def from_protobuf(message: VectorsMessage) -> VectorTracker:
        new_tracker = VectorTracker(
            message.rows,
            message.truncation,
            message.decay,
            _deserialize_ndarray(message.U)
            _deserialize_ndarray(message.S)
        )
        return new_tracker

    def to_summary(self) -> NumberSummary:
        return None
