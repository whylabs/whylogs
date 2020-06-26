"""
"""
import setuptools
import os

VERSION = "0.1.0"

REQUIREMENTS = [
    'protobuf>=3.12.2',
    'pyyaml>=5.3.1',
    'pandas>1.0',
    'numpy>=1.18',
    'datasketches>=2.0.0+1.g29a1d69'
]
DEV_EXTRA_REQUIREMENTS = [
    'ipython',
    'argh>=0.26',
    'pytest-runner>=5.2',
    'pytest',
]

# def build_protobuf():
#     target = os.path.join(_MYDIR, 'build_proto.sh')
#     output = subprocess.check_output([target])
#     print('BUILDING PROTOBUF')
#     print(output.decode('utf-8'))
#
#
# # Build the local protobuf files
# build_protobuf()

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
    tests_require=['pytest'],
)
