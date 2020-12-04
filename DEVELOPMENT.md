# Developing whylogs Python

Please take a look at this doc before contributing to whylogs python.

## Code format

We use `flake8` for linting. To run flake8 lint verification:
```
make lint
```

We use [black](https://pypi.org/project/black/) to format all our code.  Before submitting a PR you'll need to format the code by running (from the repo root dir):
(note: this is currently disabled). Black is configured by `pyproject.toml`

```
black .
```

## Development Environment

1. It's recommended that you use [miniconda](https://docs.conda.io/en/latest/miniconda.html) to develop.

2. Install [tox](https://tox.readthedocs.io/en/latest/) and [https://flake8.pycqa.org/en/latest/](flake8) and [black](https://black.readthedocs.io/en/stable/)
    ```
    # note that we are install these tools globally
    pip install tox --user
    pip install flake8 --user
    pip install black --user
    ```
3. Clone the repo

4. Clean potential previous 

```
make clean
```

5. Update all the submodules (to get the protobuf definitions): 
    
    ```
    git submodule update --init --recursive
    ```

6. Create a new conda environment for whylogs development. We need Python 3.7
 (though whylogs target multiple Python versions via `tox`):
 
    ```
    conda create --name=whylogs-dev python=3.7
    conda activate whylogs-dev
    ```

7. Install dependencies

    ```
    conda install pip
    pip install -r requirements-dev.txt
    ```

8. Install whylogs in editable mode to the current python environment

    ```
    make develop
    ```
   
9. (optional) Build and serve documentation

    ```
    make docs
    make servedocs
    ```




## Testing

To run tests using the current Python environment:
```
make test
```

### Coverage 

Coverage can be checked with 

```
make coverage
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

### 1. Local development
Start with a `dev` version locally (and you can publish them as well if needed).

```
cd whylogs-python/
bump2version dev --verbose --dry-run [--allow-dirty]
bump2version dev --verbose
```

To run tests against different Python, we use tox:
```
make test-all
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

To run test using the current Python environment (assuming you have all the dependencies):
```
make test
```

To run tests using tox, which will create test environments for you, run:
```
make test-all
```

## Examples
See the `scripts/` directory for some example scripts for interacting with `whylogs-python`

See the `notebooks/` directory for some example notebooks.


## Documentation
Auto-generated documentation is handled with [sphinx](https://www.sphinx-doc.org/en/master/).  
See the `docs/` folder for more and refer to [docs/README.md](docs/README.md)

### Examples

Example scripts which help demonstrate the use of whylogs can be placed under the `examples/` folder.
Refer to [examples/README.md](examples/README.md) for more info


### Doc string format
We use the [numpydocs docstring standard](https://numpydoc.readthedocs.io/en/latest/format.html), which is human-readable
 and works with [sphinx](https://www.sphinx-doc.org/en/master/) api documentation generator.

