import os
import urllib.request as urllib

import numpy as np
import pandas as pd
from joblib import dump
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC

# Download Iris dataset and save it as csv
url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
raw_data = urllib.urlopen(url)
try:
    os.mkdir("dataset/")
except Exception as e:
    print(" 'dataset' directory already existed. Moving forward")
# Save data as csv
with open("dataset/Iris.csv", "wb") as file:
    file.write(raw_data.read())
# Load data
data = pd.read_csv("dataset/Iris.csv", header=None)
# Separating the independent variables from dependent variables
X = data.iloc[:, 0:4].values
y = data.iloc[:, -1].values
x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.30)
# Train a classifier
print("Train started.")
model = SVC()
model.fit(x_train, y_train)
print("Train finished.")
# Save the model
dump(model, "model.joblib")
print("Model saved as model.joblib")
