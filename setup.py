import codecs
import os.path
import re
import subprocess
import sys
from distutils.command.build_py import build_py_2to3 as _build_py
from distutils.command.clean import clean as _clean
from distutils.spawn import find_executable
from distutils.util import run_2to3
from glob import glob

import setuptools
import setuptools_black


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()


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
        sys.stderr.write(
            "protoc is not installed nor found in ../src.  Please compile it "
            "or install the binary package.\n"
        )
        sys.exit(-1)

    print(" ".join(protoc_command))
    if subprocess.call(protoc_command) != 0:
        sys.exit(-1)

    run_2to3(glob("{0}/*_pb2.py".format(dst_path)))


class BuildClean(_clean):
    def run(self):
        # Delete generated files in the code tree.
        for (dirpath, dirnames, filenames) in os.walk("."):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if (
                    filepath.endswith("_pb2.py")
                    or filepath.endswith(".pyc")
                    or filepath.endswith(".so")
                    or filepath.endswith(".o")
                ):
                    os.remove(filepath)
        # _clean is an old-style class, so super() doesn't work.
        _clean.run(self)


class BuildPy(_build_py):
    def run(self):
        validate_version(version)

        generate_proto("./proto/src", "src/whylogs/proto")
        # _build_py is an old-style class, so super() doesn't work.
        _build_py.run(self)


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delimiter = '"' if '"' in line else "'"
            v = line.split(delimiter)[1]
            print(f"WhyLogs version: {v}")
            return v
    else:
        raise RuntimeError("Unable to find version string.")


version = get_version("src/whylogs/_version.py")


def validate_version(v):
    branch = os.environ.get("CI_COMMIT_BRANCH")
    if branch == "mainline":
        if "b" not in v:
            raise RuntimeError(f"Invalid version string: {v} for master branch")
    elif branch == "release":
        if not re.fullmatch(r"\d+\.\d+\.\d+", v):
            raise RuntimeError(f"Invalid version string: {v} for release branch")
    else:
        print(f"Not on master or release branch: {branch}")


# Currently, all requirements will be made mandatory, but long term we could
# remove these optional requirements.  Such packages are only needed for
# certain modules, but aren't required for core WhyLogs functionality
OPTIONAL_REQS = [
    "boto3",
    "s3fs",
]

REQUIREMENTS = [
    "click>=click==7.1.2",
    "python-dateutil>=2.8.1",
    "protobuf>=3.12.2",
    "pyyaml>=5.3.1",
    "pandas>1.0",
    "marshmallow>=3.7.1",
    "numpy>=1.18",
    "whylabs-datasketches>=2.0.0b6",
] + OPTIONAL_REQS
DEV_EXTRA_REQUIREMENTS = [
    "ipython",
    "argh>=0.26",
    "pytest-runner>=5.2",
    "pytest",
    "ipykernel",
    "pyarrow",
    # 'vmprof',
    "matplotlib",
    "pre-commit",
    "bump2version",
    "twine",
    "wheel",
    "awscli >= 1.18.93",
    "setuptools_black",
    "coverage<5",  # Required for pycharm to run tests with coverage
]

# Pip setup
with open("README.md", "rt") as f:
    long_description = f.read()

setuptools.setup(
    name="whylogs",
    version=version,
    author="WhyLabs, Inc",
    author_email="development@whylogs.ai",
    description="Creating lightweight summarization for your big datasets",
    license="Apache-2.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/whylabs/whylogs-python",
    cmdclass={
        "clean": BuildClean,
        "build_py": BuildPy,
        "build": setuptools_black.BuildCommand,
    },
    package_dir={"": "src"},
    include_package_data=True,
    packages=setuptools.find_packages("src"),
    entry_points={
        "console_scripts": [
            "whylogs=whylogs.cli:main",
            "whylogs-demo=whylogs.cli:demo_main",
        ]
    },
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Operating System :: OS Independent",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Cython",
        "Topic :: Scientific/Engineering",
    ],
    python_requires=">=3.5",
    install_requires=REQUIREMENTS,
    extras_require={"dev": DEV_EXTRA_REQUIREMENTS},
    tests_require=["pytest"],
)
