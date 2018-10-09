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
import subprocess

# Workaround a Python bug in which multiprocessing's atexit handler doesn't
# play well with pytest. See http://bugs.python.org/issue15881 for details
# and this suggested workaround (comment msg170215 in the thread).
import multiprocessing

if Cython.__version__ < '0.21.0':
    raise Exception('Please upgrade to Cython 0.21.0 or newer')

MAJOR = 1
MINOR = 9
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
        for x in ['kudu/client.cpp',
                  'kudu/config.pxi',
                  'kudu/errors.cpp',
                  'kudu/schema.cpp']:
            try:
                os.remove(x)
            except OSError:
                pass

# Identify the cc version used and check for __int128 support
def generate_config_pxi(include_dirs):
    """ Generate config.pxi from config.pxi.in """
    int128_h = None
    for d in include_dirs:
        candidate = os.path.join(d, 'kudu/util/int128.h')
        if os.path.exists(candidate):
            int128_h = candidate
            break
    if int128_h is None:
        raise Exception("could not find int128.h in Kudu include dirs")
    src = os.path.join(setup_dir, 'kudu/config.pxi.in')
    dst = os.path.join(setup_dir, 'kudu/config.pxi')
    dst_tmp = dst + '.tmp'
    cc = os.getenv("CC","cc")
    subprocess.check_call([cc, "-x", "c++", "-o", dst_tmp,
                           "-E", '-imacros', int128_h, src])
    # If our generated file is the same as the prior version,
    # don't replace it. This avoids rebuilding everything on every
    # run of setup.py.
    if os.path.exists(dst) and open(dst).read() == open(dst_tmp).read():
      os.unlink(dst_tmp)
    else:
      os.rename(dst_tmp, dst)

# If we're in the context of the Kudu git repository, build against the
# latest in-tree build artifacts
if 'KUDU_HOME' in os.environ:
    kudu_home = os.environ['KUDU_HOME']
    sys.stderr.write("Using KUDU_HOME directory: %s\n" % (kudu_home,))
    if not os.path.isdir(kudu_home):
        sys.stderr.write("%s is not a valid KUDU_HOME directory" % (kudu_home,))
        sys.exit(1)

    kudu_include_dirs = [os.path.join(kudu_home, 'src')]

    if 'KUDU_BUILD' in os.environ:
        kudu_build = os.environ['KUDU_BUILD']
        sys.stderr.write("Using KUDU_BUILD directory: %s\n" % (kudu_build,))
    else:
        kudu_build = os.path.join(kudu_home, 'build', 'latest')
        sys.stderr.write("Using inferred KUDU_BUILD directory: %s/\n" % (kudu_build,))
    if not os.path.isdir(kudu_build):
        sys.stderr.write("%s is not a valid KUDU_BUILD directory" % (kudu_build,))
        sys.exit(1)

    kudu_include_dirs.append(os.path.join(kudu_build, 'src'))
    kudu_lib_dir = os.path.join(kudu_build, 'lib', 'exported')
else:
    if os.path.exists("/usr/local/include/kudu"):
        prefix = "/usr/local"
    elif os.path.exists("/usr/include/kudu"):
        prefix = "/usr"
    else:
        sys.stderr.write("Cannot find installed kudu client.\n")
        sys.exit(1)
    sys.stderr.write("Building from system prefix {0}\n".format(prefix))
    kudu_include_dirs = [prefix + "/include"]
    kudu_lib_dir = prefix + "/lib"

INCLUDE_PATHS = kudu_include_dirs
LIBRARY_DIRS = [kudu_lib_dir]
RT_LIBRARY_DIRS = LIBRARY_DIRS

generate_config_pxi(INCLUDE_PATHS)

ext_submodules = ['client', 'errors', 'schema']

extensions = []

for submodule_name in ext_submodules:
    ext = Extension('kudu.{0}'.format(submodule_name),
                    ['kudu/{0}.pyx'.format(submodule_name)],
                    libraries=['kudu_client'],
                    include_dirs=INCLUDE_PATHS,
                    library_dirs=LIBRARY_DIRS,
                    runtime_library_dirs=RT_LIBRARY_DIRS)
    extensions.append(ext)

extensions = cythonize(extensions)

write_version_py()

LONG_DESCRIPTION = open(os.path.join(setup_dir, "README.md")).read()
DESCRIPTION = "Python interface to the Apache Kudu C++ Client API"

CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: Apache Software License',
    'Topic :: Database :: Front-Ends',
    'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
    'Environment :: Console',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Cython'
]

URL = 'http://kudu.apache.org/'

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

    # Note: dependencies in tests_require should also be listed in
    # requirements.txt so that dependencies aren't downloaded at test-time
    # (when it's more difficult to override various pip installation options).
    #
    # pytest 3.3 [1] and pytest-timeout 1.2.1 [2] dropped
    # support for python 2.6.
    #
    # 1. https://docs.pytest.org/en/latest/changelog.html#id164
    # 2. https://pypi.org/project/pytest-timeout/#id5
    tests_require=['pytest >=2.8,<3.3',
                   'pytest-timeout >=1.1.0,<1.2.1'],

    install_requires=['cython >= 0.21', 'pytz', 'six'],
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    license='Apache License, Version 2.0',
    classifiers=CLASSIFIERS,
    maintainer="Apache Kudu team",
    maintainer_email="dev@kudu.apache.org",
    url=URL,
    test_suite="kudu.tests"
)
