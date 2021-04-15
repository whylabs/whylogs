src.python := $(shell find ./src -type f -name "*.py")
tst.python := $(shell find ./tests -type f -name "*.py")
src.python.pyc := $(shell find ./src -type f -name "*.pyc")
src.proto.dir := ./proto/src
src.proto := $(shell find $(src.proto.dir) -type f -name "*.proto")

dist.dir := dist
egg.dir := .eggs
build.dir := build
build.wheel := $(dist.dir)/whylogs-0.4.5.dev1.tar.gz
build.proto.dir := src/whylogs/proto
build.proto := $(patsubst $(src.proto.dir)/%.proto,$(build.proto.dir)/%_pb2.py,$(src.proto))

default: dist

.PHONY: dist clean clean-test help format lint test test-all install coverage docs default proto test-notebooks

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

clean-test: ## Remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

dist: $(build.wheel) ## Create distribution tarballs and wheels

$(build.wheel): $(src.python)
	@$(call i, Generating distribution files)
	poetry run python setup.py sdist
	poetry run python setup.py bdist_wheel
	@$(call i, Distribution files created)
	@find dist -type f

proto:$(build.proto)

$(build.proto):$(src.proto)
	@$(call i, Generating python source for protobuf)
	poetry run python setup.py proto

lint: ## check style with flake8
	poetry run tox -e flake8

format: ## format source code with black
	poetry run black .

test: dist ## run tests with pytest
	poetry run pytest

test-notebooks: ## Run tests for the notebooks
	poetry run pytest --no-cov test_notebooks/notebook_tests.py

test-all: dist ## run tests on every Python version with tox
	poetry run tox

install: ## install all dependencies with poetry
	poetry install

coverage: ## generate test coverage reports
	poetry run pytest --cov='src/.' tests/
	poetry run python -m coverage report

docs: build-proto ## generate Sphinx HTML documentation, including API docs
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
