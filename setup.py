"""
"""
import setuptools
import os

VERSION = "0.1.0"

git_ssh_cfg = os.environ.get('PIP_GIT_SSH', 'false').lower()
if git_ssh_cfg in ('false', '0', 'f'):
    git_ssh = False
    print('Accessing github via https')
elif git_ssh_cfg in ('true', '1', 't'):
    print('Accessing github via ssh')
    git_ssh = True

if git_ssh:
    datasketches_url = 'ssh://git@github.com/apache/incubator-datasketches-cpp.git'
else:
    datasketches_url = 'https://github.com/apache/incubator-datasketches-cpp.git'

DATASKETCHES_COMMIT = '68520d4987a5d95c6f3d453647c046efa7c4c5c0'
REQUIREMENTS = [
    'protobuf>=3.12.2',
    'pyyaml>=5.3.1',
    'pandas>1.0',
    'numpy>=1.18',
    f'datasketches @ git+{datasketches_url}@' + DATASKETCHES_COMMIT,  ## noqa
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
