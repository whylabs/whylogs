#! /bin/sh

git config --global --add safe.directory /workspace
export PATH=$PATH:/home/whyuser/.local/bin
cd /workspace/python
make install
make test
