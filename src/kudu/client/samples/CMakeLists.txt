# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# Client API sample executables

cmake_minimum_required(VERSION 2.8)

find_package(kuduClient REQUIRED)
include_directories(${KUDU_CLIENT_INCLUDE_DIR})

add_executable(sample sample.cc)
target_link_libraries(sample kudu_client)
