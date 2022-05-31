# Getting started

## Installing whylogs

Users can start profiling their data with whylogs in just a few steps. First, install whylogs as follows. As of May 31st, 2022, whylogs v1 is the default.

```bash
pip install whylogs
```

Minimal requirements:

- Python 3.7+ up to Python 3.10
- Windows, Linux x86_64, and MacOS 10+

## Import whylogs

```python
import whylogs as why
```

## Prepare your data

```python
import pandas as pd

df = pd.read_csv("/my/local/path/dataset.csv")
```

## Profile your data

Profiling a dataset is as simple as running the following code.

```python
results = why.log(df)
```

From here, users can take advantage of a variety of features for inspecting their profiles, visualizing their profiles, and more. See the “Examples” section for more.

## Inspect your profile

One option is to inspect your profile as a pandas DataFrame.

```python
profile_view = results.view()
profile_view.to_pandas()
```
