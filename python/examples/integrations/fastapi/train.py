import joblib
from sklearn import neighbors
from sklearn.datasets import load_iris

# Load Iris dataset
# data_iris = load_iris(as_frame=True)
data_iris = load_iris()

X, y = data_iris.data, data_iris.target

# Initalize model
knn = neighbors.KNeighborsClassifier(n_neighbors=5)

trained_model = knn.fit(X, y)

# save best model
joblib.dump(trained_model, "knn_model.pkl")
print("model saved!")
