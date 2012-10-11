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
# GTEST_LIBRARIES, the libraries to link against to use gtest.
# GTest_FOUND, If false, do not try to use gtest.

# also defined, but not for general use are
# GTEST_LIBRARY, where to find the GTest library.

set(GTEST_H gtest/gtest.h)

find_path(GTEST_INCLUDE_DIR ${GTEST_H}
  PATHS ${CMAKE_SOURCE_DIR}/thirdparty/gtest-1.6.0/include
        NO_DEFAULT_PATH
  DOC   "Path to the ${GTEST_H} file"
)

find_library(GTEST_LIBRARY NAMES gtest
  PATHS ${CMAKE_SOURCE_DIR}/thirdparty/gtest-1.6.0
        NO_DEFAULT_PATH
  DOC   "Google's framework for writing C++ tests (gtest)"
)

find_library(GTEST_MAIN_LIBRARY NAMES gtest_main
  PATHS ${CMAKE_SOURCE_DIR}/thirdparty/gtest-1.6.0
        NO_DEFAULT_PATH
  DOC   "Google's framework for writing C++ tests (gtest_main)"
)

if(GTEST_INCLUDE_DIR AND GTEST_LIBRARY AND GTEST_MAIN_LIBRARY)
  set(GTEST_LIBRARIES ${GTEST_LIBRARY} ${GTEST_MAIN_LIBRARY})
  set(GTEST_FOUND TRUE)
else(GTEST_INCLUDE_DIR AND GTEST_LIBRARY AND GTEST_MAIN_LIBRARY)
  set(GTEST_FOUND FALSE)
endif(GTEST_INCLUDE_DIR AND GTEST_LIBRARY AND GTEST_MAIN_LIBRARY)

if(GTEST_FOUND)
  if(NOT GTest_FIND_QUIETLY)
    message(STATUS "Found GTest: ${GTEST_LIBRARIES}")
  endif(NOT GTest_FIND_QUIETLY)
else(GTEST_FOUND)
  if(NOT GTest_FIND_QUIETLY)
    if(GTest_FIND_REQUIRED)
      message(FATAL_ERROR "Could not find the GTest Library")
    else(GTest_FIND_REQUIRED)
      message(STATUS "Could not find the GTest Library")
    endif(GTest_FIND_REQUIRED)
  endif(NOT GTest_FIND_QUIETLY)
endif(GTEST_FOUND)

mark_as_advanced(GTEST_INCLUDE_DIR GTEST_LIBRARIES)

