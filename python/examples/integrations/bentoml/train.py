import bentoml
from sklearn import datasets, neighbors

# Load training data set
iris = datasets.load_iris()
X, y = iris.data, iris.target

# Create & train the model
knn = neighbors.KNeighborsClassifier(n_neighbors=5)
knn.fit(X, y)

# Save model to the BentoML local model store
saved_model = bentoml.sklearn.save_model(
    "iris_knn",
    knn,
    signatures={
        "predict": {"batchable": True, "batch_dim": 0},
        "predict_proba": {"batchable": True, "batch_dim": 0},
    },
)

print(f"Model saved: {saved_model}")
