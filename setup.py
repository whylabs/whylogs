# -*- coding: utf-8 -*-
"""
    Setup file for whylogs_python.
    Use setup.cfg to configure your project.
"""

import subprocess
import sys

import os.path
from distutils.command.clean import clean as _clean
from distutils.spawn import find_executable
from distutils.util import run_2to3
from glob import glob
from pkg_resources import VersionConflict, require
from setuptools import setup

try:
    require("setuptools>=38.3")
except VersionConflict:
    print("Error: version of setuptools is too old (<38.3)!")
    sys.exit(1)

# Find the Protocol Compiler.
if "PROTOC" in os.environ and os.path.exists(os.environ["PROTOC"]):
    protoc = os.environ["PROTOC"]
elif os.path.exists("../src/protoc"):
    protoc = "../src/protoc"
elif os.path.exists("../src/protoc.exe"):
    protoc = "../src/protoc.exe"
elif os.path.exists("../vsprojects/Debug/protoc.exe"):
    protoc = "../vsprojects/Debug/protoc.exe"
elif os.path.exists("../vsprojects/Release/protoc.exe"):
    protoc = "../vsprojects/Release/protoc.exe"
else:
    protoc = find_executable("protoc")


def generate_proto(sr_path, dst_path):
    """Invokes the Protocol Compiler to generate a _pb2.py from the given
    .proto file.
    Also, run 2to3 conversion to make the package compatible with Python 3 code."""
    proto_files = glob("{0}/*.proto".format(sr_path))
    if len(proto_files) == 0:
        sys.stderr.write("Unable to locate proto source files")
        sys.exit(-1)

    protoc_command = [
        protoc,
        "-I",
        sr_path,
        "--python_out={}".format(dst_path),
    ] + proto_files
    if protoc is None:
        sys.stderr.write("protoc is not installed nor found in ../src.  Please compile it " "or install the binary package.\n")
        sys.exit(-1)

    print(" ".join(protoc_command))
    if subprocess.call(protoc_command) != 0:
        sys.exit(-1)

    run_2to3(glob("{0}/*_pb2.py".format(dst_path)))


class BuildProto(_clean):
    def run(self):
        generate_proto("./proto/src", "src/whylogs/proto")


if __name__ == "__main__":
    setup(cmdclass={"proto": BuildProto})
