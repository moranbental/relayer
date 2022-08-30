
# Copyright 2020 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from setuptools import setup, find_packages
import re

PACKAGE_NAME = "relayer"
VERSION_FILE = PACKAGE_NAME + '/_version.py'
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"

def version():
    with open(VERSION_FILE) as fp:
        for line in fp:
            if line.startswith('__version__'):
                mo = re.search(VSRE, line, re.M)
                if mo:
                    return mo.group(1)
                raise RuntimeError("Unable to find version string in %s." % (VERSION_FILE,))

def load_deps(file_name):
    """Load dependencies from requirements file"""
    deps = []
    with open(file_name) as fp:
        for line in fp:
            line = line.strip()
            if not line or line[0] == '#' or line.startswith('-r'):
                continue
            deps.append(line)
    return deps

install_requires = load_deps('requirements.txt')
tests_require = load_deps('requirements_dev.txt')

with open("README.md") as fh:
    long_description = fh.read()

setup(
    name=PACKAGE_NAME,
    version=version(),
    description='',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Iguazio',
    author_email='moranb@iguazio.com',
    license='MIT',
    url='https://github.com/v3io/relayer',
    packages=find_packages(),
    install_requires=install_requires,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries',
    ],
    tests_require=tests_require,
)
