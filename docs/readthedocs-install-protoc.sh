#!/bin/bash -e
cd $HOME
PROTOC_ZIP=protoc-3.7.1-osx-x86_64.zip
curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v3.7.1/$PROTOC_ZIP
# Unzip the protoc binary to a directory in the PATH and accessible to this user
unzip -o $PROTOC_ZIP -d $HOME/.pyenv/bin bin/protoc
unzip -o $PROTOC_ZIP -d $HOME/.pyenv/bin 'include/*'
rm -f $PROTOC_ZIP
