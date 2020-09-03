# Developing WhyLogs Python

Please take a look at this doc before contributing to WhyLogs python.


## Installation

1. Clone the repo
2. Update all the submodules (to get the protobuf definitions): 
    
    ```git submodule update --init --recursive```

3. (optional) Create and activate a new [virtual env](https://docs.python.org/3/library/venv.html#creating-virtual-environments) or [conda env](https://docs.python.org/3/library/venv.html#creating-virtual-environments)

4. Install dependencies

    ```pip install -r requirements-dev.txt```

5. Install whylogs in editable mode to the current python environment

    ```pip install -e .[dev]```
   
6. (optional) Install sphinx documentation generation dependencies

    ```cd docs && pip install -r requirements.txt```


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

### 1. Local development
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

### 2. Pushing to master branch

* If you are planning to push to `master` branch, please first create a dev version (see the above guide). 
**You'll have to bump it to a `beta` version or the build will fail**. You'll need to do this before creating the merge request:
```
bump2version beta --verbose --dry-run
bump2version beta --verbose
```

### Updating notebooks
Before committing any changes to the example notebooks, you should clear all cell outputs.
We don't want to version control the notebook cell outputs.


### 3. Full release

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


## Documentation
Auto-generated documentation is handled with [sphinx](https://www.sphinx-doc.org/en/master/).  
See the `docs/` folder for more and refer to [docs/README.md](docs/README.md)

### Examples
Example scripts which help demonstrate the use of WhyLogs can be placed under the `examples/` folder.
Refer to [examples/README.md](examples/README.md) for more info


### Doc string format
We use the [numpydocs docstring standard](https://numpydoc.readthedocs.io/en/latest/format.html), which is human-readable and works with [sphinx](https://www.sphinx-doc.org/en/master/) api documentation generator.

