from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.corpus import inaugural

import numpy as np

from whylogs.cor.statistics import NLPTracker


# inverted index weighting utility functions

def global_freq(A: np.ndarray) -> np.ndarray:
    gf = np.zeros(A.shape[0])
    for i in range(A.shape[0]):
        for j in range(A.shape[1]):
            gf[i] += A[i,j]
    return gf


def entropy(A: np.ndarray) -> np.ndarray:
    gf = global_freq(A)
    g = np.ones(A.shape[0])
    logN = np.log(A.shape[1])
    assert logN > 0.0
    for i in range(A.shape[0]):
        assert gf[i] > 0.0
        for j in range(A.shape[1]):
            p_ij = A[i,j] / gf[i]
            g[i] += p_ij * np.log(p_ij) / logN if p_ij > 0.0 else 0.0
    return g


def log_entropy(A: np.ndarray) -> None:
    g = entropy(A)
    for i in range(A.shape[0]):
        for j in range(A.shape[1]):
            A[i,j] = g[i] * np.log(A[i,j] + 1.0)


# the NLTK tokenizer produces punctuation as terms, so stop them
stop_words = set(stopwords.words("english") + ['.', ',', ':', ';', '."', ',"', '"', "'", ' ', '?',
                                               '[', ']', '.]', "' ", '" ', '?', '? ', '-', '- ', '/',
                                               '?"', ''])

# build weighted inverted index of inaugural speeches
stemmer = PorterStemmer()

vstopped = {w for w in inaugural.words() if w.casefold() not in stop_words}
vocab = {stemmer.stem(w.casefold()) for w in vstopped}
vocab_size = len(vocab)

vocab_map = {}
dim = 0
for w in vocab:
    if w not in vocab_map:
        vocab_map[w] = dim
        dim += 1

ndocs = len(inaugural.fileids())
doc = 0
index = np.zeros((vocab_size, ndocs))
for fid in inaugural.fileids():
    stopped = [t.casefold() for t in inaugural.words(fid) if t.casefold() not in stop_words]
    stemmed = [stemmer.stem(w) for w in stopped]
    for w in stemmed:
        index[vocab_map[w], doc] += 1
    doc += 1

A = index.copy()
log_entropy(A)

gf = global_freq(index)
g = entropy(index)

# build reference profile in vector tracker

num_concepts = 100
ref_tracker = VectorTracker(vocab_size, num_concepts)
for fid in inaugural.fileids():
    stopped = [t.casefold() for t in inaugural.words(fid) if t.casefold() not in stop_words]
    stemmed = [stemmer.stem(w) for w in stopped]
    doc_vec = np.zeros(vocab_size)
    for w in stemmed:
        doc_vec[vocab_map[w]] += 1
    for i in range(vocab_size):
        doc_vec[i] = g[i] * np.log(doc_vec[i] + 1.0)
    ref_tracker.track(doc_vec)

# save reference profile locally
write_me: NLPTrackerMessage = ref_tracker.to_protobuf()


# production tracking, no reference update

ref_tracker = VectorTracker.from_protobuf(write_me)
prod_tracker = NLPTracker()
for fid in inaugural.fileids():
    stopped = [t.casefold() for t in inaugural.words(fid) if t.casefold() not in stop_words]
    stemmed = [stemmer.stem(w) for w in stopped]
    doc_vec = np.zeros(vocab_size)
    for w in stemmed:
        doc_vec[vocab_map[w]] += 1
    for i in range(vocab_size):
        doc_vec[i] = g[i] * np.log(doc_vec[i] + 1.0)
    # ref_tracker.track(doc_vec) if you want...
    prod_tracker.track(doc_vec, ref_tracker.similarity(doc_vec))

# send to whylabs, no SVD state

send_me: NLPTrackerMessage = prod_tracker.to_protobuf()

# get stats on doc length, term length, SVD "fit"
view_me: NLPSummary = prod_tracker.to_summary()

# write_me = ref_tracker.to_protobuf() write new SVD state if updated
