#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from Cython.Distutils import build_ext
from Cython.Build import cythonize
import Cython

import sys
from setuptools import setup
from distutils.command.clean import clean as _clean
from distutils.extension import Extension
import os

if Cython.__version__ < '0.19.1':
    raise Exception('Please upgrade to Cython 0.19.1 or newer')

MAJOR = 0
MINOR = 1
MICRO = 0
VERSION = '%d.%d.%d' % (MAJOR, MINOR, MICRO)
ISRELEASED = True

setup_dir = os.path.abspath(os.path.dirname(__file__))


def write_version_py(filename=os.path.join(setup_dir, 'kudu/version.py')):
    version = VERSION
    if not ISRELEASED:
        version += '.dev'

    a = open(filename, 'w')
    file_content = "\n".join(["",
                              "# THIS FILE IS GENERATED FROM SETUP.PY",
                              "version = '%(version)s'",
                              "isrelease = '%(isrelease)s'"])

    a.write(file_content % {'version': VERSION,
                            'isrelease': str(ISRELEASED)})
    a.close()


class clean(_clean):
    def run(self):
        _clean.run(self)
        for x in ['kudu/client.cpp', 'kudu/schema.cpp',
                  'kudu/errors.cpp']:
            try:
                os.remove(x)
            except OSError:
                pass


# If we're in the context of the Kudu git repository, build against the
# latest in-tree build artifacts
if ('KUDU_HOME' in os.environ and
        os.path.exists(os.path.join(os.environ['KUDU_HOME'],
                                    "build/latest"))):
    sys.stderr.write("Building from in-tree build artifacts\n")
    kudu_include_dir = os.path.join(os.environ['KUDU_HOME'], 'src')
    kudu_lib_dir = os.path.join(os.environ['KUDU_HOME'],
                                'build/latest/exported')
else:
    if os.path.exists("/usr/local/include/kudu"):
        prefix = "/usr/local"
    elif os.path.exists("/usr/include/kudu"):
        prefix = "/usr"
    else:
        sys.stderr.write("Cannot find installed kudu client.\n")
        sys.exit(1)
    sys.stderr.write("Building from system prefix {0}\n".format(prefix))
    kudu_include_dir = prefix + "/include"
    kudu_lib_dir = prefix + "/lib"

INCLUDE_PATHS = [kudu_include_dir]
LIBRARY_DIRS = [kudu_lib_dir]
RT_LIBRARY_DIRS = LIBRARY_DIRS

ext_submodules = ['client', 'errors', 'schema']

extensions = []

for submodule_name in ext_submodules:
    ext = Extension('kudu.{0}'.format(submodule_name),
                    ['kudu/{0}.pyx'.format(submodule_name)],
                    libraries=['kudu_client'],
                    # Disable the 'new' gcc5 ABI; see the top-level
                    # CMakeLists.txt for details.
                    define_macros=[('_GLIBCXX_USE_CXX11_ABI', '0')],
                    include_dirs=INCLUDE_PATHS,
                    library_dirs=LIBRARY_DIRS,
                    runtime_library_dirs=RT_LIBRARY_DIRS)
    extensions.append(ext)

extensions = cythonize(extensions)

write_version_py()

LONG_DESCRIPTION = open(os.path.join(setup_dir, "README.md")).read()
DESCRIPTION = "Python interface to the Apache Kudu (incubating) C++ Client API"

CLASSIFIERS = [
    'Development Status :: 3 - Alpha',
    'Environment :: Console',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Cython'
]

setup(
    name="kudu-python",
    packages=['kudu', 'kudu.tests'],
    version=VERSION,
    package_data={'kudu': ['*.pxd', '*.pyx']},
    ext_modules=extensions,
    cmdclass={
        'clean': clean,
        'build_ext': build_ext
    },
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    install_requires=['cython >= 0.21'],
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    license='Apache License, Version 2.0',
    classifiers=CLASSIFIERS,
    author="Apache Kudu (incubating) team",
    maintainer_email="dev@kudu.incubator.apache.org",
    test_suite="kudu.tests"
)
