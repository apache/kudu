#!/bin/bash -e
# Copyright (c) 2015, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# Script which embeds a piece of raw data into an ELF object file (.o).
# This also null-terminates the data.
if [ "$#" != 2 ]; then
  echo "usage: $0 <input.data> <output.o>"
  exit 1
fi

INPUT=$1
ABS_OUTPUT=$(readlink -f $2)

# Set up a temporary directory to make our NULL-terminated copy in.
TMPDIR=$(mktemp -d)
cleanup() {
  rm -Rf $TMPDIR
}
trap cleanup EXIT

# Make the null-terminated copy.
TMP_INPUT=$TMPDIR/$(basename $INPUT)
cp $INPUT $TMP_INPUT
printf "\0" >> $TMP_INPUT

# Have to set the working directory and use a non-absolute path so that the
# generated symbol name is consistent. Otherwise, the symbol name would
# depend on the build directory.
cd $TMPDIR
ld -r -b binary -o $ABS_OUTPUT $(basename $INPUT)
