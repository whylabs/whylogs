# Guidelines for contributing

## Table of Contents <!-- omit in toc -->

- [Summary](#summary)
  - [Contributors](#contributors)
  - [Maintainers](#maintainers)
- [Git](#git)
- [Python](#python)
  - [Python code style](#python-code-style)
  - [Poetry](#poetry)
- [Docker](#docker)

## Summary

### Contributors

**PRs welcome!**

- **Consider starting a [discussion](https://docs.github.com/en/discussions) to see if there's interest in what you want to do.**
- **Submit PRs from feature branches on forks.**
- **Ensure PRs pass all CI checks.**
- **Maintain or increase test coverage.**

### Maintainers

- **Make `develop` the default branch.**
- **Merge PRs into `develop`.** Configure repository settings so that branches are deleted automatically after PRs are merged.
- **Only merge to `main` if [fast-forwarding](https://www.git-scm.com/book/en/v2/Git-Branching-Basic-Branching-and-Merging) from `develop`.**
- **Enable [branch protection](https://docs.github.com/en/free-pro-team@latest/github/administering-a-repository/about-protected-branches) on `develop` and `main`.**
- **Set up a release workflow.** Here's an example release workflow, controlled by Git tags:
  - Bump the version number in `pyproject.toml` with `poetry version` and commit the changes to `develop`.
  - Push to `develop` and verify all CI checks pass.
  - Fast-forward merge to `main`, push, and verify all CI checks pass.
  - Create an [annotated and signed Git tag](https://www.git-scm.com/book/en/v2/Git-Basics-Tagging)
    - Follow [SemVer](https://semver.org/) guidelines when choosing a version number.
    - List PRs and commits in the tag message:
      ```sh
      git log --pretty=format:"- %s (%h)" "$(git describe --abbrev=0 --tags)"..HEAD
      ```
    - Omit the leading `v` (use `1.0.0` instead of `v1.0.0`)
    - Example: `git tag -a -s 1.0.0`
  - Push the tag. GitHub Actions will build and push the Python package and Docker images.

## Git

- _[Why use Git?](https://www.git-scm.com/about)_ Git enables creation of multiple versions of a code repository called branches, with the ability to track and undo changes in detail.
- Install Git by [downloading](https://www.git-scm.com/downloads) from the website, or with a package manager like [Homebrew](https://brew.sh/).
- [Configure Git to connect to GitHub with SSH](https://docs.github.com/en/free-pro-team@latest/github/authenticating-to-github/connecting-to-github-with-ssh)
- [Fork](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo) this repo
- Create a [branch](https://www.git-scm.com/book/en/v2/Git-Branching-Branches-in-a-Nutshell) in your fork.
- Commit your changes with a [properly-formatted Git commit message](https://chris.beams.io/posts/git-commit/).
- Create a [pull request (PR)](https://docs.github.com/en/free-pro-team@latest/github/collaborating-with-issues-and-pull-requests/about-pull-requests) to incorporate your changes into the upstream project you forked.

## Python

### Python code style

- Python code is formatted with [Black](https://black.readthedocs.io/en/stable/). Configuration for Black is stored in _pyproject.toml_.
- Python imports are organized automatically with [isort](https://pycqa.github.io/isort/).
  - The isort package organizes imports in three sections:
    1. Standard library
    2. Dependencies
    3. Project
  - Within each of those groups, `import` statements occur first, then `from` statements, in alphabetical order.
  - You can run isort from the command line with `poetry run isort .`.
  - Configuration for isort is stored in _pyproject.toml_.
- Other web code (JSON, Markdown, YAML) is formatted with [Prettier](https://prettier.io/).
- Code style is enforced with [pre-commit](https://pre-commit.com/), which runs [Git hooks](https://www.git-scm.com/book/en/v2/Customizing-Git-Git-Hooks).

  - Configuration is stored in _.pre-commit-config.yaml_.
  - Pre-commit can run locally before each commit (hence "pre-commit"), or on different Git events like `pre-push`.
  - Pre-commit is installed in the Poetry environment. To use:

    ```sh
    # after running `poetry install`
    path/to/repo
    ❯ poetry shell

    # install hooks that run before each commit
    path/to/repo
    .venv ❯ pre-commit install

    # and/or install hooks that run before each push
    path/to/repo
    .venv ❯ pre-commit install --hook-type pre-push
    ```

  - Pre-commit is also useful as a CI tool. The GitHub Actions workflows run pre-commit hooks with [GitHub Actions](https://github.com/features/actions).

### Poetry

This project uses [Poetry](https://python-poetry.org/) for dependency management.

#### Highlights

- **Automatic virtual environment management**: Poetry automatically manages the `virtualenv` for the application.
- **Automatic dependency management**: rather than having to run `pip freeze > requirements.txt`, Poetry automatically manages the dependency file (called _pyproject.toml_), and enables SemVer-level control over dependencies like [npm](https://semver.npmjs.com/). Poetry also manages a lockfile (called _poetry.lock_), which is similar to _package-lock.json_ for npm. Poetry uses this lockfile to automatically track specific versions and hashes for every dependency.
- **Dependency resolution**: Poetry will automatically resolve any dependency version conflicts. pip did not have dependency resolution [until the end of 2020](https://pip.pypa.io/en/latest/user_guide/#changes-to-the-pip-dependency-resolver-in-20-3-2020).
- **Dependency separation**: Poetry can maintain separate lists of dependencies for development and production in the _pyproject.toml_. Production installs can skip development dependencies to speed up Docker builds.
- **Builds**: Poetry has features for easily building the project into a Python package.

#### Installation

The recommended installation method is through the [Poetry custom installer](https://python-poetry.org/docs/#installation), which vendorizes dependencies into an isolated environment, and allows you to update Poetry with `poetry self update`.

You can also install Poetry however you prefer to install your user Python packages (`pipx install poetry`, `pip install --user poetry`, etc). Use the standard update methods with these tools (`pipx upgrade poetry`, `pip install --user --upgrade poetry`, etc).

#### Key commands

```sh
# Basic usage: https://python-poetry.org/docs/basic-usage/
poetry install  # create virtual environment and install dependencies
poetry show --tree  # list installed packages
poetry add PACKAGE@VERSION # add a package to production dependencies, like pip install
poetry add PACKAGE@VERSION --dev # add a package to development dependencies
poetry update  # update dependencies (not available with standard tools)
poetry version  # list or update version of this package
poetry shell  # activate the virtual environment, like source venv/bin/activate
poetry run COMMAND  # run a command within the virtual environment
poetry env info  # manage environments: https://python-poetry.org/docs/managing-environments/
poetry config virtualenvs.in-project true  # configure Poetry to install virtualenvs into .venv
poetry export -f requirements.txt > requirements.txt --dev  # export dependencies
```

## Docker

- **[Docker](https://www.docker.com/)** is a technology for running lightweight virtual machines called **containers**.
  - An **image** is the executable set of files read by Docker.
  - A **container** is a running image.
  - The **[Dockerfile](https://docs.docker.com/engine/reference/builder/)** tells Docker how to build the container.
- To [get started with Docker](https://www.docker.com/get-started):
  - Ubuntu Linux: follow the [instructions for Ubuntu Linux](https://docs.docker.com/install/linux/docker-ce/ubuntu/), making sure to follow the [postinstallation steps](https://docs.docker.com/install/linux/linux-postinstall/) to activate the Docker daemon.
  - macOS and Windows: install [Docker Desktop](https://www.docker.com/products/docker-desktop) (available via [Homebrew](https://brew.sh/) with `brew cask install docker`).
- <details><summary>Expand this details element for more <a href="https://docs.docker.com/engine/reference/commandline/cli/">useful Docker commands</a>.</summary>

  ```sh
  # Log in with Docker Hub credentials to pull images
  docker login
  # List images
  docker images
  # List running containers: can also use `docker container ls`
  docker ps
  # View logs for the most recently started container
  docker logs -f $(docker ps -q -n 1)
  # View logs for all running containers
  docker logs -f $(docker ps -aq)
  # Inspect a container (web in this example) and return the IP Address
  docker inspect web | grep IPAddress
  # Stop a container
  docker stop # container hash
  # Stop all running containers
  docker stop $(docker ps -aq)
  # Remove a downloaded image
  docker image rm # image hash or name
  # Remove a container
  docker container rm # container hash
  # Prune images
  docker image prune
  # Prune stopped containers (completely wipes them and resets their state)
  docker container prune
  # Prune everything
  docker system prune
  # Open a shell in the most recently started container (like SSH)
  docker exec -it $(docker ps -q -n 1) /bin/bash
  # Or, connect as root:
  docker exec -u 0 -it $(docker ps -q -n 1) /bin/bash
  # Copy file to/from container:
  docker cp [container_name]:/path/to/file destination.file
  ```

  </summary>
