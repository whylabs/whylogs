from io import BytesIO

import numpy as np

from .numbertracker import NumberTracker
from whylogs.proto import NumbersMessage, NumberSummary, VectorsMessage


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

    def __init__(self, rows: int, k: int, decay: float=1.0) -> None:
        assert rows > 1
        assert 0.0 < decay <= 1.0
        assert k > 1
        # assert compatible if other is not None
        # to cope with open vocabularies, we need to zero pad the _sketch
        #   np.pad(self._sketch, ((0, <new vocab size>), (0,0)), 'constant')

        self._rows = rows
        self._decay = decay
        self._k = k
        self._U = np.zeros((rows, k))
        self._S = np.zeros(k)
        self.similarity = NumberTracker()

    def _resketch(self, U1: np.ndarray, S1: np.ndarray) -> None:
        U0, S0 = self._U, self._S
        Q, R = np.linalg.qr(np.concatenate((decay * U0 * S0, U1 * S1), axis=1))
        UR, self._S, VRT = np.linalg.svd(R)
        self._U = np.dot(Q, UR)

    def track(self, vector: np.ndarray, update_reference: bool=True) -> None:
        assert vector.shape[0] == self._rows # need to reshape for open vocabularies
        if update_reference:
            U1, S1, _ = np.linalg.svd(vector.reshape((self._rows, 1)), False, True, False)
            self._resketch(U1, S1)

        # similarity = 1 - || U S S^+ U' vector - vector || / || vector ||
        # S S^+ could probably just be I with the appropriate singular values zeroed
        similarity = self._U.transpose * vector
        similarity = _reciprocal(self._S) * similarity
        similarity = self._S * similarity
        similarity = self._U * similarity
        similarity = similarity - vector
        similarity = 1.0 - np.linalg.norm(similarity) / np.linalg.norm(vector)
        self.similarity.track(similarity)

    def merge(self, other):
        # assert compatible
        new_tracker = VectorTracker(self._rows, self._k, self._decay)
        new_tracker.similarity.merge(self.similarity)
        new_tracker.similarity.merge(other.similarity)
        new_tracker._resketch(self._U, self._S)
        new_tracker._resketch(other._U, other._S)
        return new_tracker

    def to_protobuf(self, include_vector_state: bool=False) -> VectorsMessage:
        return VectorsMessage(
            similarity = self.similarity.to_protobuf(),
            rows = self._rows,
            truncation = self._k,
            decay = self._decay,
            U = _serialize_ndarray(self._U) if include_vector_state else None,
            S = _serialize_ndarray(self._S) if include_vector_state else None,
        )

    @staticmethod
    def from_protobuf(message: VectorsMessage) -> VectorTracker:
        new_tracker = VectorTracker(
            message.rows,
            message.truncation,
            message.decay,
        )
        new_tracker.similarity = NumberTracker.from_protobuf(message.similarity)
        if message.U and message.S:
            new_tracker._U = _deserialize_ndarray(message.U)
            new_tracker._S = _deserialize_ndarray(message.S)
        return new_tracker

    def to_summary(self) -> NumberSummary:
        return self.similarity.to_summary()
