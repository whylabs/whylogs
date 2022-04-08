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
        similarity: Optional[NumberTracker] = None,
        frequent_items: Optional[frequent_strings_sketch] = None,
    ) -> None:
        self.doc_length = doc_length or NumberTracker()
        self.term_length = term_length or NumberTracker()
        self.similarity = similarity or NumberTracker()
        self.frequent_terms = frequent_terms or frequent_strings_sketch()

    def track(self, terms: Optional[List[str]]=None, similarity: float) -> None:
        if terms is not None:
            self.doc_length.track(len(terms))
            for term in terms:
                self.term_length.track(len(term))
                self.frequent_terms.update(term)
        self.similarity.track(similarity)

    def merge(self, other: NLPTracker) -> NLPTracker:
        new_doc_length = self.doc_length.merge(other.doc_length)
        new_term_length = self.term_length.merge(other.term_length)
        new_similarity = self.similarity.merge(other.similarity)
        new_frequent_terms = self.frequent_terms.merge(other.frequent_terms)
        return NLPTracker(new_doc_length, new_term_length, new_similarity, new_frequent_terms)

    def to_protobuf(self) -> NLPTrackerMessage:
        return NLPTrackerMessage(
            doc_length = self.doc_length.to_protobuf() if self.doc_length else None,
            term_length = self.term_length.to_protobuf() if self.term_length else None,
            similarity = self.similarity.to_protbuf() if self.similarity else None,
            frequent_terms = self.frequent_terms.serialize() if self.frequent_terms else None,
        )

    @staticmethod
    def from_protobuf(message: NLPTrackerMessage) -> NLPTracker:
        return NLPTracker(
            doc_length = NumberTracker.from_protobuf(message.doc_length),
            term_length = NumberTracker.from_protobuf(mesage.term_length),
            similarity = NumberTracker.from_protobuf(message.similarity),
            frequent_terms = dsketch.deserialize_frequent_strings_sketch(message.frequent_terms),
        )

    def to_summary(self) -> NLPSummary:
        return NLPSummary(
            doc_length = self.doc_length.to_summary(),
            term_length = self.term_length.to_summary(),
            similarity = self.similarity.to_summary(),
            frequent_terms = from_string_sketch(self.frequent_terms),
        )
