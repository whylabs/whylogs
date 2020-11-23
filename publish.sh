#!/bin/sh

rm -fr build/*
rm -fr dist/*
python setup.py bdist_wheel
twine upload dist/*
