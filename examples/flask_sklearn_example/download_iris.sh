#!/bin/bash
mkdir dataset && cd dataset
kaggle datasets download -d uciml/iris
unzip iris.zip