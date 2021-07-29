
# Environment Setup
You will need to the protobuf compilers. In MacOs you can use brew to install it.
```
brew install protobuf
```

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

Poetry manages virtualenvs as well. Typically, on a project that uses virtualenv directly you would activate the virtualenv to get all of the binaries that you install with pip onto the path. Poetry works in a similar way but with different commands.

```
# Activate the poetry virtualenv
poetry shell
```

You shouldn't have to actually do that unless you want to run some of the executables that are installed from pypi directly. One use case would be manually running `pytest` on a single test file rather than running them all with `make test`.

## Manging Dependencies
This is done through poetry. There needs be a good reason to add dependencies that should be included in the commit message.

```bash
# Add a dependency
poetry add package-name

# Add a dev only dependency that gets installed locally during dev
poetry add --dev package-name

# Update the lock file. Run this after adding or removing things. It doesn't appear to always  happen automatically.
poetry lock
```

# Development Details
You can run `make help` for a full list of targets that you can run. These are the ones that you'll need most often.

```bash
# Build everything, producing a source tar and wheel in ./dist
make

# For running tests locally
make test

# For running notebook tests
make test-notebooks

# For formatting and linting
make lint
make lint-fix
make format
make format-fix

# Remove all generated artifacts
make clean

# Run all pre-push checks: linter, style checker, testing, etc.
make release
```

## IDE Setup
There are a few useful plugins that are probably available for most IDEs. Using Pyrcharm, you'll want to install the Poetry and black plugins.

- [blackconnect](https://plugins.jetbrains.com/plugin/14321-blackconnect) can be configured to auto format files on save. Just run `make blackd` from a shell to set up the server and the plugin will do its thing. You need to configure it to format on save, it's off by default.
- [poetry](https://plugins.jetbrains.com/plugin/14307-poetry) plugin will make the dependencies installed by poetry available in pycharm.

# Publishing
Running `make publish` will upload a new version to pypi using poetry's publish logic. Follow the Push Process to bump the version appropriately.

# Push Process
Before pushing you should run `make release`. That will run all of the formatting, linting, testing, and building that will happen in CI. If that fails then you'll know before pushing it off to a branch. You can also reproduce what happens on CI servers using the `act` tool. See the details in the section below. The process in general is

- `make release` to make sure it builds.
- bump the version using `make bump-patch`, `make bump-minor`, `make bump-major`, `make bump-release`, or `make bump-dev`, depending on the magnitude of the change. This will update all files that mention the version number. Commit those changes if they look right or reach out of you're not sure what to do.
    - Patch is for small bug fixes.
    - Minor is for new features.
    - Major is for breaking changes.
- If this is a new feature then add some examples to `examples/`
- Push to a branch on Github.
- Submit a pull request to the main repo.

## Run CI locally with `act`
You can run local github actions on ubuntu using [act](https://github.com/nektos/act). Currently, you need to build the latest docker image for ubuntu using the following dockerfile

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



