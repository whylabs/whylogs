import logging
from typing import List, Optional

import numpy as np

from datasketches import frequent_strings_sketch

from whylogs.core.summaryconverters import from_string_sketch

from .numbertracker import NumberTracker

from whylogs.proto import NumbersMessage, NumberSummary, NLPTrackerMessage, NLPSummary
from whylogs.util import dsketch

logger = logging.getLogger(__name__)


class NLPTracker:
    """
    Track statistics for NLP documents
    """

    def __init__(
        self,
        doc_length: Optional[NumberTracker] = None,
        term_length: Optional[NumberTracker] = None,
        term_vectors: Optional[VectorTracker] = None,
        frequent_items: Optional[frequent_strings_sketch] = None,
        vocab_size: int = 0,
        truncation: int = 0,
        decay: float = 0,
    ) -> None:
        assert vocab_size or term_vectors
        vocab_size = term_vectors._rows if term_vectors else vocab_size
        assert truncation or term_vectors
        truncation = term_vectors._k if term_vectors else truncation
        assert decay or term_vectors
        decay = term_vectors._decay if term_vectors else decay
        assert 0 < decay <= 1
        self.doc_length = doc_length if doc_length else NumberTracker()
        self.term_length = term_length if term_length else NumberTracker()
        self.term_vectors = term_vectors if term_vectors else VectorTracker(vocab_size, truncation, decay)
        self.frequent_terms = frequent_terms if frequent_terms else frequent_strings_sketch()

    def track(self, terms: Optional[List[str]]=None, vector: Optional[np.ndarray]=None, update_reference: bool=True) -> None:
        if terms is not None:
            self.doc_length.track(len(terms))
            for term in terms:
                self.term_length.track(len(term))
                self.frequent_terms.update(term)
        if vector is not None:
            self.track(vector, update_reference)

    def merge(self, other: NLPTracker) -> NLPTracker:
        new_doc_length = self.doc_length.merge(other.doc_length)
        new_term_length = self.term_length.merge(other.term_length)
        new_term_vectors = self.term_vectors.merge(other.term_vectors)
        new_frequent_terms = self.frequent_terms.merge(other.frequent_terms)
        return NLPTracker(new_doc_length, new_term_length, new_term_vectors, new_frequent_terms)

    def to_protobuf(self, include_vector_state: bool=False) -> NLPTrackerMessage:
        return NLPTrackerMessage(
            doc_length = self.doc_length.to_protobuf() if self.doc_length else None,
            term_length = self.term_length.to_protobuf() if self.term_length else None,
            term_vectors = self.term_vectors.to_protbuf(include_vector_state) if self.term_vectors else None,
            frequent_terms = self.frequent_terms.serialize() if self.frequent_terms else None,
        )

    @staticmethod
    def from_protobuf(message: NLPTrackerMessage) -> NLPTracker:
        return NLPTracker(
            doc_length = NumberTracker.from_protobuf(message.doc_length),
            term_length = NumberTracker.from_protobuf(mesage.term_length),
            term_vectors = VectorTracker.from_protobuf(message.term_vectors),
            frequent_terms = dsketch.deserialize_frequent_strings_sketch(message.frequent_terms),
        )

    def to_summary(self) -> NLPSummary:
        return NLPSummary(
            doc_length = self.doc_length.to_summary(),
            term_length = self.term_length.to_summary(),
            similarity = self.term_vectors.to_summary(),
            frequent_terms = from_string_sketch(self.frequent_terms),
        )
