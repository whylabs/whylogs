src.python := $(shell find ./whylogs_v1 -type f -name "*.py")
tst.python := $(shell find ./tests -type f -name "*.py")
src.python.pyc := $(shell find ./whylogs_v1 -type f -name "*.pyc")
src.proto.dir := ../proto/src
src.proto := $(shell find $(src.proto.dir) -type f -name "*.proto")

version := 1.0.0

dist.dir := dist
egg.dir := .eggs
build.dir := build
# This isn't exactly true but its the only thing that we easily know the name of at this point. Its a good proxy for
# the wheel since its created along with it.
build.wheel := $(dist.dir)/whylogs-v1-$(version).tar.gz
build.proto.dir := whylogs_v1/core/proto
build.proto := $(patsubst $(src.proto.dir)/%.proto,$(build.proto.dir)/%_pb2.py,$(src.proto))

default: dist

release: format lint test dist ## Compile distribution files and run all tests and checks.

pre-commit:
	@$(call i, Running pre-commit checks)
	poetry run pre-commit run --all-files


.PHONY: dist clean clean-test help format lint test install coverage docs default proto github release
.PHONY: test-system-python format-fix blackd jupyter-kernel

ifeq ($(shell which poetry), )
	$(error "Can't find poetry on the path. Install it at https://python-poetry.org/docs.")
endif

install-poetry:
	@$(call i, Installing Poetry)
	curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

publish: clean dist ## Clean the project, generate new distribution files and publish them to pypi
	@$(call i, Publishing the currently built dist to pypi)
	poetry publish

blackd:
	@$(call i, Running the black server)
	poetry run blackd

clean: clean-test ## Remove all build artifacts
	rm -rf $(dist.dir)
	rm -rf $(build.dir)
	rm -f $(src.python.pyc)
	rm -rf $(egg.dir)
	rm -rf $(build.proto)
	rm -f $(build.proto)
	rm -f requirements.txt

clean-test: ## Remove test and coverage artifacts
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
	$(eval MYPY := $(shell poetry run which protoc-gen-mypy))
	protoc -I $(src.proto.dir) --plugin=protoc-gen-mypy=$(MYPY) --python_out=$(build.proto.dir) $(src.proto) --mypy_out=$(build.proto.dir)

lint: ## Check code for lint errors.
	@$(call i, Running the linter)
	poetry run flake8

lint-fix: ## Automatically fix linting issues.
	@$(call i, Running the linter)
	poetry run autoflake --in-place --remove-all-unused-imports --remove-unused-variables $(src.python) $(tst.python)

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
	python -m poetry run pytest -vv --cov='whylogs_v1/.' tests/

install: ## Install all dependencies with poetry.
	@$(call i, Installing dependencies)
	poetry install

coverage: ## Generate test coverage reports.
	@$(call i, Generating test coverage)
	poetry run pytest --cov='whylogs_v1/.' tests/
	poetry run python -m coverage report

jupyter-kernel: ## Install a kernel for this workspace in Jupyter. You should have jupyterlab installed on your system.
	@$(call i, Installing a kernel for this workspace for Jupyter)
	poetry run python -m ipykernel install --user --name=whylogs-v1-dev

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
python highlight/colors.py INFO "$1"
echo
endef

define w
echo
python highlight/colors.py WARN "$1"
echo
endef

define e
echo
python highlight/colors.py ERROR "$1"
echo
endef
