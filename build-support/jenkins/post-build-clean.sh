#!/bin/bash -x
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# Script which runs on Jenkins slaves after the build/test to clean up
# disk space used by build artifacts. Our build tends to "make clean"
# before running anyway, so doing the post-build cleanup shouldn't
# hurt our build times. It does, however, save a fair amount of disk
# space in the Jenkins workspace disk. This can help prevent our EC2
# slaves from filling up and causing spurious failures.

ROOT=$(readlink -f $(dirname "$BASH_SOURCE")/../..)
cd $ROOT

# Note that we use simple shell commands instead of "make clean"
# or "mvn clean". This is more foolproof even if something ends
# up partially compiling, etc.

# Clean up intermediate object files in the src tree
find src -name \*.o -exec rm -f {} \;

# Clean up the actual build artifacts
rm -Rf build/latest/

# Clean up any java build artifacts
find java -name \*.jar -delete -o -name \*.class -delete 
