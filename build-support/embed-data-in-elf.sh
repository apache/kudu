#!/bin/bash -e
# Copyright 2015 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
