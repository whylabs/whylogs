#! /bin/sh

git config --global --add safe.directory /workspace
export PATH=$PATH:/home/whyuser/.local/bin
cd /workspace/python
whoami
id
mount
df .
ls -alh
sudo touch foo
sudo make install
sudo make test
