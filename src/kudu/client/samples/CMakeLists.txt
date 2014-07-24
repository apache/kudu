# Copyright (c) 2014, Cloudera, inc.
#
# Client API sample executables

cmake_minimum_required(VERSION 2.8)

find_package(client REQUIRED)
include_directories(${CLIENT_INCLUDE_DIR})

add_executable(sample sample.cc)
target_link_libraries(sample client)
