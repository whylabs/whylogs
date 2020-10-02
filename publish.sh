#!/bin/sh

rm -fr dist/*
python setup.py bdist_wheel
twine upload dist/*
