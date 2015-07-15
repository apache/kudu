#!/usr/bin/env python

# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

from Cython.Distutils import build_ext
from Cython.Build import cythonize
import Cython

if Cython.__version__ < '0.19.1':
    raise Exception('Please upgrade to Cython 0.19.1 or newer')
import sys
from setuptools import setup
from distutils.extension import Extension
import os

MAJOR = 0
MINOR = 0
MICRO = 1
VERSION = '%d.%d.%d' % (MAJOR, MINOR, MICRO)

from distutils.command.clean import clean as _clean

class clean(_clean):
    def run(self):
        _clean.run(self)
        for x in ['kudu/client.cpp']:
            try:
                os.remove(x)
            except OSError:
                pass


# If we're in the context of the Kudu git repository, build against the
# latest in-tree build artifacts
if 'KUDU_HOME' in os.environ and \
  os.path.exists(os.path.join(os.environ['KUDU_HOME'], "build/latest")):
    print >>sys.stderr, "Building from in-tree build artifacts"
    kudu_include_dir = os.path.join(os.environ['KUDU_HOME'], 'src')
    kudu_lib_dir = os.path.join(os.environ['KUDU_HOME'], 'build/latest/exported')
else:
    if os.path.exists("/usr/local/include/kudu"):
        prefix="/usr/local"
    elif os.path.exists("/usr/include/kudu"):
        prefix="/usr"
    else:
        print >>sys.stderr, "Cannot find installed kudu client."
        sys.exit(1)
    print >>sys.stderr, "Building from system prefix ", prefix
    kudu_include_dir = prefix + "/include"
    kudu_lib_dir = prefix + "/lib"

INCLUDE_PATHS = [kudu_include_dir]
LIBRARY_DIRS = [kudu_lib_dir]
RT_LIBRARY_DIRS = LIBRARY_DIRS

client_ext = Extension('kudu.client', ['kudu/client.pyx'],
                       libraries=['kudu_client'],
                       include_dirs=INCLUDE_PATHS,
                       library_dirs=LIBRARY_DIRS,
                       runtime_library_dirs=RT_LIBRARY_DIRS)

extensions = cythonize([client_ext])

setup(
    name="python-kudu",
    packages=["kudu"],
    version=VERSION,
    setup_requires=['nose>=1.0'],
    package_data={'kudu': ['*.pxd', '*.pyx']},
    ext_modules=extensions,
    cmdclass = {
        'clean': clean,
        'build_ext': build_ext
    },
    install_requires=['cython >= 0.21'],
    description="Cython wrapper for the Kudu C++ API",
    license='Proprietary',
    author="Wes McKinney",
    maintainer_email="wes@cloudera.com",
    test_suite="kudu.tests"
)
