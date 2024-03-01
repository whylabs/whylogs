#! /bin/sh

git config --global --add safe.directory /workspace
export PATH=$PATH:/home/whyuser/.local/bin
cd /workspace/python
whoami
id
mount
df .
ls -alh
echo $PATH
which poetry
sudo touch foo
sudo make install
sudo make test
