#! /bin/sh

git config --global --add safe.directory /workspace
export PATH=$PATH:/home/whyuser/.local/bin

cd /workspace/python
make install
make test pre-commit && poetry run pytest tests/api/writer/test_whylabs_integration.py --load
