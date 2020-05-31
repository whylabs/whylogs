"""
"""
import subprocess

import setuptools

VERSION = "0.1.0"
DATASKETCHES_COMMIT = '68520d4987a5d95c6f3d453647c046efa7c4c5c0'
REQUIREMENTS = [
    'protobuf',
    '2to3',
    'pyyaml',
    'datasketches @ git+https://github.com/apache/incubator-datasketches-cpp.git@' + DATASKETCHES_COMMIT,  ## noqa
]
DEV_EXTRA_REQUIREMENTS = [
    'numpy',
    'ipython',
    'pandas',
    'argh',
    'pytest',
]


def build_protobuf():
    output = subprocess.check_output(['./build_proto.sh'])
    print('BUILDING PROTOBUF')
    print(output.decode('utf-8'))


# Build the local protobuf files
build_protobuf()

# Pip setup
with open('README.md', 'rt') as f:
    long_description = f.read()
setuptools.setup(
    name='whylogs-python',
    version=VERSION,
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
)
