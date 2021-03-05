.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	rm -fr src/whylogs/proto/*pb2.py
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

lint: ## check style with flake8
	tox -e flake8

test: build-proto ## run tests quickly with the default Python
	pytest

test-all: build-proto ## run tests on every Python version with tox
	tox

coverage: ## check code coverage quickly with the default Python
	pytest --cov='src/.' tests/
	python -m coverage report

docs: build-proto ## generate Sphinx HTML documentation, including API docs
	cd docs
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) build/sphinx/html/index.html

servedocs: docs ## compile the docs watching for changes
	watchmedo shell-command -p '*.rst' -recursive -c '$(MAKE) -C docs' -D .

release: dist ## package and upload a release
	twine upload dist/*

dist: clean-build build-proto ## builds source and wheel package
	python setup.py build
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist

install: build-proto clean ## install the package to the active Python's site-packages
	python setup.py install

build-proto:
	python setup.py proto

build: build-proto lint
	python setup.py build

develop: build-proto
	python setup.py develop