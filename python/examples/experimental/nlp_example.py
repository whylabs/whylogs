import numpy as np
from nltk.corpus import inaugural, stopwords
from nltk.stem import PorterStemmer

from whylogs.core.configs import SummaryConfig
from whylogs.experimental.core.metrics.nlp_metric import (
    NlpLogger,
    SvdMetric,
    SvdMetricConfig,
    UpdatableSvdMetric,
)

# from nltk.tokenize import word_tokenize

# import nltk
# nltk.download('stopwords')
# nltk.download('inaugural')

# inverted index weighting utility functions


def global_freq(A: np.ndarray) -> np.ndarray:
    gf = np.zeros(A.shape[0])
    for i in range(A.shape[0]):
        for j in range(A.shape[1]):
            gf[i] += A[i, j]
    return gf


def entropy(A: np.ndarray) -> np.ndarray:
    gf = global_freq(A)
    g = np.ones(A.shape[0])
    logN = np.log(A.shape[1])
    assert logN > 0.0
    for i in range(A.shape[0]):
        assert gf[i] > 0.0
        for j in range(A.shape[1]):
            p_ij = A[i, j] / gf[i]
            g[i] += p_ij * np.log(p_ij) / logN if p_ij > 0.0 else 0.0
    return g


def log_entropy(A: np.ndarray) -> None:
    g = entropy(A)
    for i in range(A.shape[0]):
        for j in range(A.shape[1]):
            A[i, j] = g[i] * np.log(A[i, j] + 1.0)


# the NLTK tokenizer produces punctuation as terms, so stop them
stop_words = set(
    stopwords.words("english")
    + [
        ".",
        ",",
        ":",
        ";",
        '."',
        ',"',
        '"',
        "'",
        " ",
        "?",
        "[",
        "]",
        ".]",
        "' ",
        '" ',
        "? ",
        "-",
        "- ",
        "/",
        '?"',
        "...",
        "",
    ]
)

# build weighted inverted index of inaugural speeches
stemmer = PorterStemmer()

vstopped = {w for w in inaugural.words() if w.casefold() not in stop_words}
vocab = {stemmer.stem(w.casefold()) for w in vstopped}
vocab_size = len(vocab)

vocab_map = {}
rev_map = [""] * vocab_size
dim = 0
for w in vocab:
    if w not in vocab_map:
        vocab_map[w] = dim
        rev_map[dim] = w
        dim += 1

doc_lengths = []
ndocs = len(inaugural.fileids())
doc = 0
index = np.zeros((vocab_size, ndocs))
for fid in inaugural.fileids():
    stopped = [t.casefold() for t in inaugural.words(fid) if t.casefold() not in stop_words]
    stemmed = [stemmer.stem(w) for w in stopped]
    doc_lengths.append(len(stemmed))
    for w in stemmed:
        index[vocab_map[w], doc] += 1
    doc += 1


A = index.copy()
log_entropy(A)

gf = global_freq(index)
g = entropy(index)


# build reference profile

num_concepts = 10
old_doc_decay_rate = 1.0
svd_config = SvdMetricConfig(k=num_concepts, decay=old_doc_decay_rate)
nlp_logger = NlpLogger(svd_class=UpdatableSvdMetric, svd_config=svd_config)

for fid in inaugural.fileids():
    stopped = [t.casefold() for t in inaugural.words(fid) if t.casefold() not in stop_words]
    stemmed = [stemmer.stem(w) for w in stopped]

    doc_vec = np.zeros(vocab_size)
    for w in stemmed:
        doc_vec[vocab_map[w]] += 1
    for i in range(vocab_size):
        doc_vec[i] = g[i] * np.log(doc_vec[i] + 1.0)

    nlp_logger.log(stemmed, doc_vec)

svd = nlp_logger._svd_metric


concepts = svd.U.value.transpose()
for i in range(concepts.shape[0]):
    pos_idx = sorted(range(len(concepts[i])), key=lambda x: concepts[i][x])[-10:]
    neg_idx = sorted(range(len(concepts[i])), key=lambda x: -1 * concepts[i][x])[-5:]
    print(", ".join([rev_map[j] for j in pos_idx]))  # + [rev_map[j] for j in neg_idx]))
print()

# save reference profile locally
send_me_to_whylabs = nlp_logger.get_profile()  # small--only has a few standard metrics (no SVD)
svd_write_me = nlp_logger.get_svd_state()  # big--contains the SVD approximation & parameters


# production tracking, no reference update

prod_logger = NlpLogger(svd_class=SvdMetric, svd_state=svd_write_me)  # use UpdatableSvdMetric to train in production

prod_svd = prod_logger._svd_metric

residuals = []
for fid in inaugural.fileids():
    stopped = [t.casefold() for t in inaugural.words(fid) if t.casefold() not in stop_words]
    stemmed = [stemmer.stem(w) for w in stopped]

    doc_vec = np.zeros(vocab_size)
    for w in stemmed:
        doc_vec[vocab_map[w]] += 1
    for i in range(vocab_size):
        doc_vec[i] = g[i] * np.log(doc_vec[i] + 1.0)

    residuals.append(prod_svd.residual(doc_vec))
    prod_logger.log(stemmed, doc_vec)  # update residual only, not SVD

print(f"\nresiduals: {residuals}\n")

# if we trained with production data
# svd_write_me = prod_logger.get_svd_state()

# send to whylabs, no SVD state
send_me = prod_logger.get_profile()

# get stats on doc length, term length, SVD "fit"
view_me = prod_svd.to_summary_dict(SummaryConfig())
print(view_me)
