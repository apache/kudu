# Copyright (c) 2009-2010 Volvox Development Team
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Author: Konstantin Lepa <konstantin.lepa@gmail.com>
#
# Find the Google Test Framework
#
# This module defines
# GTEST_INCLUDE_DIR, where to find gtest include files, etc.
# GTest_FOUND, If false, do not try to use gtest.
# GTEST_STATIC_LIBRARY, Location of libgtest.a
# GTEST_SHARED_LIBRARY, Location of libgtest's shared library

find_path(GTEST_INCLUDE_DIR gtest/gtest.h
  DOC   "Path to the gtest header file"
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(GTEST_SHARED_LIBRARY gtest
  DOC   "Google's framework for writing C++ tests (gtest)"
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(GTEST_STATIC_LIBRARY libgtest.a
  DOC   "Google's framework for writing C++ tests (gtest) static"
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

# Kudu does not use the gtest_main library (we have kudu_test_main).
#find_library(GTEST_MAIN_LIBRARY_PATH
#  NAMES gtest_main
#  PATHS GTEST_SEARCH_PATH
#        NO_DEFAULT_PATH
#  DOC   "Google's framework for writing C++ tests (gtest_main)"
#)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GTest REQUIRED_VARS
  GTEST_SHARED_LIBRARY GTEST_STATIC_LIBRARY GTEST_INCLUDE_DIR)
