src.python := $(shell find ./src -type f -name "*.py")
tst.python := $(shell find ./tests -type f -name "*.py")
src.python.pyc := $(shell find ./src -type f -name "*.pyc")
src.proto.dir := ./proto/src
src.proto := $(shell find $(src.proto.dir) -type f -name "*.proto")

dist.dir := dist
egg.dir := .eggs
build.dir := build
# This isn't exactly true but its the only thing that we easily know the name of at this point. Its a good proxy for
# the wheel since its created along with it.
build.wheel := $(dist.dir)/whylogs-0.4.5.dev1.tar.gz
build.proto.dir := src/whylogs/proto
build.proto := $(patsubst $(src.proto.dir)/%.proto,$(build.proto.dir)/%_pb2.py,$(src.proto))

default: dist

release:
	# Run format checker
	# Run dist

github:
	# TODO

.PHONY: dist clean clean-test help format lint test install coverage docs default proto test-notebooks github release test-system-python

ifeq (, $(shell which poetry))
	$(error "Can't find poetry on the path. Install it at https://python-poetry.org/docs.")
endif

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-test ## Remove all build artifacts
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

lint: ## check style with flake8
	@$(call i, Running the linter)
	poetry run tox -e flake8

format: ## format source code with black
	@$(call i, Running the formatter)
	poetry run black .

test: dist ## run tests with pytest
	@$(call i, Running tests)
	poetry run pytest

test-system-python: dist ## Run tests using the system `python` instead of the locally declared poetry python
	@$(call i, Running tests using the globally installed python)
	python -m poetry run python --version
	python -m poetry run pytest

test-notebooks: ## Run tests for the notebooks
	@$(call i, Running notebook tests)
	poetry run pytest --no-cov test_notebooks/notebook_tests.py

install: ## install all dependencies with poetry
	@$(call i, Installing dependencies)
	poetry install

coverage: ## generate test coverage reports
	@$(call i, Generating test coverage)
	poetry run pytest --cov='src/.' tests/
	poetry run python -m coverage report

docs: proto ## generate Sphinx HTML documentation, including API docs
	@$(call i, Generating docs)
	rm -f docs/whylogs.rst
	rm -f docs/modules.rst
	cd docs
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) build/sphinx/html/index.html


define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT
BROWSER := python -c "$$BROWSER_PYSCRIPT"

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
echo "[INFO] $1"
echo
endef

define w
echo
echo "[WARN] $1"
echo
endef

define e
echo
echo "[ERROR] $1"
echo
endef
