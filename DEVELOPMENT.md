# Developing whylogs Python

Please take a look at this doc before contributing to whylogs python.



## Code format

The following run the linter (flake8) and formatter (black). It fails if it finds issues but they won't be automatically resolved.

```bash
make lint format
```

To automatically resolve issues, run

```bash
make format-fix
```

## Development Environment

You'll need to install poetry in order to install dependencies using the lock file in this project. Follow [their docs](https://python-poetry.org/docs/) to get it set up.

```
git clone https://github.com/whylabs/whylogs-java.git
git submodule update --init --recursive

# Use poetry to install dependencies
make install

# Build the protobuf source and the distribution tar and wheel
make

# For building and viewing the docs if you're working on those
make docs
```

Poetry manages virtualenvs as well. Typically, on a project that uses virtualenv directly you would activate the virtualenv to get all of the binaries that you install with pip onto the path. Poetry works in a similar way but with different comands.

```
# Activate the poetry virtualenv
poetry shell
```

You shouldn't have to actually do that unless you want to run some of the things that are pip installed directly. One use case would be manually running `pytest` on a single test file rather than running them all with `make test`.

## Managing Dependencies

Poetry manages all of the dependencies. If you need to add something then you should run the following.

```
poetry add new-dependency

# Or for dev/test only dependencies
poetry add --dev new-dependency

# Update the lock file after
poetry lock
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

### Testing CI locally

TODO make this a real docker file then

you can run local github actions on ubuntu using [act](https://github.com/nektos/act). Currently, you need to build the latest docker image for ubuntu using the following dockerfile

```dockerfile
FROM ubuntu:20.04
ENV LC_CTYPE=en_US.UTF-8
RUN apt-get update \
    && apt-get upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    git \
    build-essential \
    curl \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*
```
if you tag the above docker image as `ubuntu-builder`, then simply run at the root of the project

```
act -P ubuntu-latest=ubuntu-builder
```
It will run all the tests in ubuntu, currently act does not support matrix.os runs on mac-os or windows

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

## Documentation
Auto-generated documentation is handled with [sphinx](https://www.sphinx-doc.org/en/master/).  
See the `docs/` folder for more and refer to [docs/README.md](docs/README.md)

### Examples

Example scripts which help demonstrate the use of whylogs can be placed under the `examples/` folder.
Refer to [examples/README.md](examples/README.md) for more info


### Doc string format
We use the [numpydocs docstring standard](https://numpydoc.readthedocs.io/en/latest/format.html), which is human-readable
 and works with [sphinx](https://www.sphinx-doc.org/en/master/) api documentation generator.

