#!/bin/bash

# Install compilers and basic build environment
yum groupinstall -y 'Development Tools'

# Install CMake for C++ examples
yum install -y cmake python-devel
