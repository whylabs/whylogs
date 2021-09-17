import pandas as pd
from joblib import dump
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split

data = pd.read_csv('dataset/Iris.csv')
# Separating the independent variables from dependent variables
X = data.drop(['Id', 'Species'], axis=1)
y = data['Species']
x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.30)
# Train a classifier
print("Train started...")
model = SVC()
model.fit(x_train, y_train)
print("Train ended...")
# Save the model
dump(model, 'model.joblib')
print("Model saved!")