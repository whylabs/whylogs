from typing import Optional

import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA


class ReferenceSelector:
    def __init__(self):
        self.n_references = 0
        self.ref_labels = None

    def calculate_references(self, X: np.ndarray, y: Optional[np.ndarray]):
        raise NotImplementedError()


class PCACentroidsSelector(ReferenceSelector):
    def __init__(self, n_components: int = 2, lower_percentile_limit: float = 0, upper_percentile_limit: float = 0):
        super().__init__()
        self.n_components = n_components
        self.lower_percentile_limit = lower_percentile_limit
        self.upper_percentile_limit = upper_percentile_limit

    def calculate_references(self, X: np.ndarray, y: Optional[np.ndarray]):
        # Fit PCA
        pca = PCA(n_components=self.n_components)
        X_pca = pca.fit_transform(X)

        # Find centroids for each label in PCA space
        n_labels = np.unique(y).shape[0]
        refs = [None] * n_labels
        labels = sorted(np.unique(y))
        self.n_references = len(labels)
        self.ref_labels = labels
        for i, label in enumerate(labels):
            filtered_X_pca = X_pca[y == label]
            if self.lower_percentile_limit != 0 or self.upper_percentile_limit != 0:
                lp = np.percentile(filtered_X_pca, self.lower_percentile_limit)
                up = np.percentile(filtered_X_pca, self.upper_percentile_limit)
                filtered_X_pca = filtered_X_pca[(lp < filtered_X_pca) & (filtered_X_pca < up)]
            refs[i] = filtered_X_pca.mean(axis=0)
        refs = np.array(refs).tolist()

        # Convert centroids back to raw space
        raw_refs = pca.inverse_transform(refs)

        return raw_refs, self.ref_labels


class PCAKMeansSelector(ReferenceSelector):
    def __init__(self, n_clusters: int = 8, n_components: int = 2, kmeans_kwargs={}):
        super().__init__()
        self.n_clusters = n_clusters
        self.n_components = n_components
        self.kmeans_kwargs = kmeans_kwargs

    def calculate_references(self, X: np.ndarray, y: Optional[np.ndarray] = None):
        self.n_references = self.n_clusters
        self.ref_labels = list(range(self.n_clusters))

        # Fit PCA first
        pca = PCA(n_components=self.n_components)
        X_pca = pca.fit_transform(X)

        # Find k-means clusters
        kmeans = KMeans(n_clusters=self.n_clusters, **self.kmeans_kwargs)
        kmeans.fit(X_pca)
        refs = kmeans.cluster_centers_

        # Convert centroids back to raw space
        raw_refs = pca.inverse_transform(refs)

        return raw_refs, self.ref_labels
