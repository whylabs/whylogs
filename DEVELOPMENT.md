# WhyLogs Python

Python port of the [WhyLogs Java library](https://gitlab.com/whylabs/whylogs-java)

## Installation
Currently, `python 3.7` is recommended.

### 1. Login to private pypi

#### 1a. Install aws cli
```pip install --upgrade awscli```

#### 1b. Login to private PYPI
You'll need access to the private PYPI repo.
Scripts for logging into the private pypi are available at the [why-shell repo](https://gitlab.com/whylabs/why-shell)


### 2. Install whylogs

#### From private PYPI
```pip install whylogs-python```

#### OR install from the whylogs repo (required for dev)
This installation also requires access to the private PYPI

1. Clone the repo and cd into the directory
2. Build the protobuf files
```./gradlew build-python```
3. cd into `whylogs-python` and install with pip.

For a dev installation with development requirements, it's recommended create a fresh conda environment or virtualenv

 ```
# Development installation
pip install -v -e ".[dev]"
 ```

 Dev install with local package
 ```
 # Development installation
 pip install -v -e ".[dev]"
 pip install -v -e /path/to/whylogs-python/
 ```

Standard installation:

 ```
 # Standard installation
 pip install .
 ```
 
 ## Release process
 * If you are doing development locally, use the following command to create a local dev version. 
 
 Some basic guideline for choosing whether it's `patch|minor|major`:
 * Patch is for small bug fixes
 * Minor is for new features
 * Major is for breaking changes / API
 
The flow looks like this:
```
 check out master -> create branch -> make changes -> bump dev -> publish dev
 -> bump beta -> create merge request ->  merge in to master 
 -> check out master -> bump release -> merge request to release -> merge into release
```

### Local development
Start with a `dev` version locally (and you can publish them as well if needed).

```
cd whylogs-python/
bump2version dev --verbose --dry-run [--allow-dirty]
bump2version dev --verbose
```
Then from the root of the repo
```
./gradlew publish-python
```

You can keep bumping the local version if you need to (you can't republish a version twice so this is needed).

### Pushing to master branch

* If you are planning to push to `master` branch, please first create a dev version (see the above guide). 
**You'll have to bump it to a `beta` version or the build will fail**. You'll need to do this before creating the merge request:
```
bump2version beta --verbose --dry-run
bump2version beta --verbose
```

### Updating notebooks
Before committing any changes to the example notebooks, you should clear all cell outputs.
We don't want to version control the notebook cell outputs.


### Full release

For full release, you'll have to bump it to a release version and push to `release` branch. This branch
will contain only 'nice-looking' version string (i.e. `1.0.1`). Doing otherwise will fail the build when merging into `release` branch.
```
bump2version release --verbose --dry-run
bump2version release --verbose
```

## Tests
Testing is handled with the `pytest` framework.
You can run all the tests by running `pytest -vvs tests/` from the parent directory.

## Examples
See the `scripts/` directory for some example scripts for interacting with `whylogs-python`

See the `notebooks/` directory for some example notebooks.


# Development/contribution
## Doc string format
We use the [numpydocs docstring standard](https://numpydoc.readthedocs.io/en/latest/format.html), which is human-readable and works with [sphinx](https://www.sphinx-doc.org/en/master/) api documentation generator.

# Data

## Example column and daset summaries
Check the `example-data/` folder for some example summary files.

### Flat dataset summary mapping
The following dictionary details the mapping from the nested `DatasetSummary` attributes to the flat table names:
```python
from whylogs.core.datasetprofile import SCALAR_NAME_MAPPING
import json
print(json.dumps(SCALAR_NAME_MAPPING, indent=2))


{
  "counters": {
    "count": "count",
    "null_count": {
      "value": "null_count"
    },
    "true_count": {
      "value": "bool_count"
    }
  },
  "number_summary": {
    "count": "numeric_count",
    "max": "max",
    "mean": "mean",
    "min": "min",
    "stddev": "stddev",
    "unique_count": {
      "estimate": "nunique_numbers",
      "lower": "nunique_numbers_lower",
      "upper": "nunique_numbers_upper"
    }
  },
  "schema": {
    "inferred_type": {
      "type": "inferred_dtype",
      "ratio": "dtype_fraction"
    },
    "type_counts": {
      "UNKNOWN": "type_unknown_count",
      "NULL": "type_null_count",
      "FRACTIONAL": "type_fractional_count",
      "INTEGRAL": "type_integral_count",
      "BOOLEAN": "type_boolean_count",
      "STRING": "type_string_count"
    }
  },
  "string_summary": {
    "unique_count": {
      "estimate": "nunique_str",
      "lower": "nunique_str_lower",
      "upper": "ununique_str_upper"
    }
  }
}
```

#### Counts
* count: total number of records for that column.  
  Note that if dictionary records are being parsed (e.g. `{'col_a': val_a, 'col_b': val_b}`) any missing fields will not be counted at all.   They won't contribute to null counts at all.  This behavior may change in the future.
* if you want type counts, I'd stick with the `type_*` fields.  Again, nulls only get counted if an actually null value is passed for a given record.