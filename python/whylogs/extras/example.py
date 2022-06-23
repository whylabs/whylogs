import numpy as np
from nltk.corpus import inaugural, stopwords
from nltk.stem import PorterStemmer

from whylogs.core.configs import SummaryConfig
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.extras.nlp_metric import NlpMetric, NlpConfig, SvdMetric, SvdMetricConfig, UpdatableSvdMetric, _preprocessifier

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
        "?",
        "? ",
        "-",
        "- ",
        "/",
        '?"',
        "",
    ]
)

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


# build reference profile

num_concepts = 100
old_doc_decay_rate = 0.8
svd_config = SvdMetricConfig(num_concepts, old_doc_decay_rate)
svd = UpdatableSvdMetric.zero(svd_config)

nlp_config = NlpConfig(svd)
ref_nlp = NlpMetric.zero(nlp_config)

for fid in inaugural.fileids():
    stopped = [t.casefold() for t in inaugural.words(fid) if t.casefold() not in stop_words]
    stemmed = [stemmer.stem(w) for w in stopped]

    doc_vec = np.zeros(vocab_size)
    for w in stemmed:
        doc_vec[vocab_map[w]] += 1
    for i in range(vocab_size):
        doc_vec[i] = g[i] * np.log(doc_vec[i] + 1.0)

    ref_nlp.columnar_update(_preprocessifier(stemmed, doc_vec))  # update SVD & residual

# save reference profile locally
write_me = ref_nlp.to_protobuf()  # small--only has 3 DistributionMetrics and a FrequentItemsMetric
svd_write_me = ref_nlp.svd.to_protobuf()  # big--contains the SVD approximation & parameters


# production tracking, no reference update

svd = SvdMetric.from_protobuf(svd_write_me)  # use UpdatableSvdMetric to train in production
nlp_config = NlpConfig(svd)
prod_nlp = NlpMetric.zero(nlp_config)

for fid in inaugural.fileids():
    stopped = [t.casefold() for t in inaugural.words(fid) if t.casefold() not in stop_words]
    stemmed = [stemmer.stem(w) for w in stopped]

    doc_vec = np.zeros(vocab_size)
    for w in stemmed:
        doc_vec[vocab_map[w]] += 1
    for i in range(vocab_size):
        doc_vec[i] = g[i] * np.log(doc_vec[i] + 1.0)

    prod_nlp.columnar_update(_preprocessifier(stemmed, doc_vec))  # update residual only, not SVD

# if we trained with production data
# write_me = prod_nlp.svd.to_protobuf()

# send to whylabs, no SVD state
send_me = prod_nlp.to_protobuf()

# get stats on doc length, term length, SVD "fit"
view_me = prod_nlp.to_summary_dict(SummaryConfig())
print(view_me)
