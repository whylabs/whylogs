import re

import setuptools
import codecs
import os.path


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delimiter = '"' if '"' in line else "'"
            v = line.split(delimiter)[1]
            print(f'WhyLogs version: {v}')
            return v
    else:
        raise RuntimeError("Unable to find version string.")


version = get_version("src/whylabs/logs/_version.py")


def validate_version(v):
    branch = os.environ.get("CI_COMMIT_BRANCH")
    if branch == 'master':
        if 'b' not in v:
            raise RuntimeError(f'Invalid version string: {v} for master branch')
    elif branch == 'release':
        if not re.fullmatch(r'\d+\.\d+\.\d+', v):
            raise RuntimeError(f'Invalid version string: {v} for release branch')
    else:
        print(f'Not on master or release branch: {branch}')


validate_version(version)

# Currently, all requirements will be made mandatory, but long term we could
# remove these optional requirements.  Such packages are only needed for
# certain modules, but aren't required for core WhyLogs functionality
OPTIONAL_REQS = [
    'boto3',
    's3fs',
]

REQUIREMENTS = [
    'protobuf>=3.12.2',
    'pyyaml>=5.3.1',
    'pandas>1.0',
    'numpy>=1.18',
    'datasketches==2.0.0b1'
] + OPTIONAL_REQS
DEV_EXTRA_REQUIREMENTS = [
    'ipython',
    'argh>=0.26',
    'pytest-runner>=5.2',
    'pytest',
    'ipykernel',
    'pyarrow',
    # 'vmprof',
    'matplotlib',
    'bump2version',
    'twine',
    'wheel',
]

# Pip setup
with open('README.md', 'rt') as f:
    long_description = f.read()

setuptools.setup(
    name='whylogs-python',
    version=version,
    author='WhyLabs, Inc',
    author_email='info@whylabs.ai',
    description='WhyLogs data monitoring library',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://gitlab.com/whylabs/whylogs-python',
    package_dir={'': 'src'},
    packages=setuptools.find_packages('src'),
    # classifiers=['TBD'],
    python_requires='>=3.5',  # TODO: Figure out python version compatibility,
    install_requires=REQUIREMENTS,
    extras_require={'dev': DEV_EXTRA_REQUIREMENTS},
    tests_require=['pytest'],
)
