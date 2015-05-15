#!/usr/bin/env python

# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

from Cython.Distutils import build_ext
from Cython.Build import cythonize
import Cython

if Cython.__version__ < '0.19.1':
    raise Exception('Please upgrade to Cython 0.19.1 or newer')
import sys
from setuptools.command.test import test as TestCommand
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


# The below class is a verbatim copy from https://pytest.org/latest/goodpractises.html
class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # Import here, because outside the eggs aren't loaded.
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


# TODO: a more portable method for this
kudu_include_dir = os.path.join(os.environ['KUDU_HOME'], 'src')
kudu_lib_dir = os.path.join(os.environ['KUDU_HOME'], 'build/latest')

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
    tests_require=['pytest'],
    version=VERSION,
    package_data={'kudu': ['*.pxd', '*.pyx']},
    ext_modules=extensions,
    cmdclass = {
        'clean': clean,
        'build_ext': build_ext,
        'test' : PyTest
    },
    install_requires=['cython >= 0.21'],
    description="Cython wrapper for the Kudu C++ API",
    license='Proprietary',
    author="Wes McKinney",
    maintainer_email="wes@cloudera.com",
    test_suite="kudu.tests"
)
