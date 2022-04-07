src.python := $(shell find ./src -type f -name "*.py")
tst.python := $(shell find ./tests -type f -name "*.py")
tst.notebooks.python := $(shell find ./test_notebooks -type f -name "*.py")
src.python.pyc := $(shell find ./src -type f -name "*.pyc")
src.proto.dir := ./proto/src
src.proto := $(shell find $(src.proto.dir) -type f -name "*.proto")

version := 0.7.3

dist.dir := dist
egg.dir := .eggs
build.dir := build
# This isn't exactly true but its the only thing that we easily know the name of at this point. Its a good proxy for
# the wheel since its created along with it.
build.wheel := $(dist.dir)/whylogs-$(version).tar.gz
build.proto.dir := src/whylogs/proto
build.proto := $(patsubst $(src.proto.dir)/%.proto,$(build.proto.dir)/%_pb2.py,$(src.proto))

default: dist

release: format lint test dist ## Compile distribution files and run all tests and checks.

pre-commit: format-fix lint-fix release

.PHONY: dist clean clean-test help format lint test install coverage docs default proto test-notebooks github release
.PHONY: test-system-python format-fix bump-patch bump-minor bump-major publish bump-dev bump-build bump-release blackd
.PHONY: jupyter-kernel

ifeq ($(shell which poetry), )
	$(error "Can't find poetry on the path. Install it at https://python-poetry.org/docs.")
endif

install-poetry:
	@$(call i, Installing Poetry)
	curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

bump-patch: ## Bump the patch version (_._.X) everywhere it appears in the project
	@$(call i, Bumping the patch number)
	poetry run bumpversion patch --allow-dirty

bump-minor: ## Bump the minor version (_.X._) everywhere it appears in the project
	@$(call i, Bumping the minor number)
	poetry run bumpversion minor --allow-dirty

bump-major: ## Bump the major version (X._._) everywhere it appears in the project
	@$(call i, Bumping the major number)
	poetry run bumpversion major --allow-dirty

bump-release: ## Convert the version into a release variant (_._._) everywhere it appears in the project
	@$(call i, Bumping the major number)
	poetry run bumpversion release --allow-dirty

bump-dev: ## Convert the version into a dev variant (_._._-dev__) everywhere it appears in the project
	@$(call i, Bumping the major number)
	poetry run bumpversion dev --allow-dirty

bump-build: ## Bump the build number (_._._-____XX) everywhere it appears in the project
	@$(call i, Bumping the major number)
	poetry run bumpversion build --allow-dirty

publish: clean dist ## Clean the project, generate new distribution files and publish them to pypi
	@$(call i, Publishing the currently built dist to pypi)
	poetry publish

blackd:
	@$(call i, Running the black server)
	poetry run blackd

clean: clean-test ## Remove all build artifacts
	rm -f docs/whylogs.rst
	rm -f docs/modules.rst
	rm -rf $(dist.dir)
	rm -rf $(build.dir)
	rm -f $(src.python.pyc)
	rm -rf $(egg.dir)
	rm -rf $(build.proto)
	rm -f $(build.proto)
	rm -f requirements.txt

clean-test: ## Remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

dist: $(build.wheel) ## Create distribution tarballs and wheels

$(build.wheel): $(src.python) $(build.proto)
	@$(call i, Generating distribution files)
	poetry build
	@$(call i, Distribution files created)
	@find dist -type f

proto: $(build.proto)

requirements.txt:
	@$(call i, Generating a requirements.txt file from poetry)
	poetry export -f requirements.txt --output requirements.txt --dev

$(build.proto): $(src.proto)
	@$(call i, Generating python source for protobuf)
	protoc -I $(src.proto.dir) --python_out=$(build.proto.dir) $(src.proto)
	poetry run 2to3 --nobackups --write ./src/whylogs/proto/

lint: ## Check code for lint errors.
	@$(call i, Running the linter)
	poetry run flake8

lint-fix: ## Automatically fix linting issues.
	@$(call i, Running the linter)
	poetry run autoflake --in-place --remove-all-unused-imports --remove-unused-variables $(src.python) $(tst.python) $(tst.notebooks.python)

format: ## Check style formatting.
	@$(call i, Checking import formatting)
	poetry run isort --check-only .
	@$(call i, Checking code formatting)
	poetry run black --check .

format-fix: ## Fix formatting with black. This updates files.
	@$(call i, Formatting imports)
	poetry run isort .
	@$(call i, Formatting code)
	poetry run black .

test: dist ## Run unit tests.
	@$(call i, Running tests)
	poetry run pytest

test-system-python: dist ## Run tests using the system `python` instead of the locally declared poetry python
	@$(call i, Running tests using the globally installed python)
	python -m poetry run python --version
	python -m poetry run pytest -vv --cov='src/.' tests/

test-notebooks: ## Run tests for the notebooks
	@$(call i, Running notebook tests)
	poetry run pytest --no-cov test_notebooks/notebook_tests.py

install: ## Install all dependencies with poetry.
	@$(call i, Installing dependencies)
	$(source HOME/.poetry/env)
	poetry install

coverage: ## Generate test coverage reports.
	@$(call i, Generating test coverage)
	poetry run pytest --cov='src/.' tests/
	poetry run python -m coverage report

docs: proto ## Generate Sphinx HTML documentation, including API docs.
	@$(call i, Generating docs)
	cd docs
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) build/sphinx/html/index.html

jupyter-kernel: ## Install a kernel for this workspace in Jupyter. You should have jupyterlab installed on your system.
	@$(call i, Installing a kernel for this workspace for Jupyter)
	poetry run python -m ipykernel install --user --name=whylogs-dev

define BROWSER_PYSCRIPT
import os, webbrowser, sys
from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT
BROWSER := poetry run python -c "$$BROWSER_PYSCRIPT"

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

define i
echo
python scripts/colors.py INFO "$1"
echo
endef

define w
echo
python scripts/colors.py WARN "$1"
echo
endef

define e
echo
python scripts/colors.py ERROR "$1"
echo
endef
