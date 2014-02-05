#!/bin/bash
##
# Portions copyright (c) 2014 Cloudera, Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
##
# script to run protoc to generate protocol buf files.
# usage: ./build-proto.sh
#

KUDU_DIR=`dirname $0`/../../..
SRC_DIR=$KUDU_DIR/src
JAVA_DIR=$KUDU_DIR/java/kudu-client/src/main/java/
PROTO_FILES=`find $SRC_DIR -type f -name "*.proto"`
PROTOC_BIN=$KUDU_DIR/thirdparty/installed/bin/protoc
if [ ! -f "$PROTOC_BIN" ] ; then
  if which protoc > /dev/null; then
    echo 'Warning: Using protoc from PATH instead of the 3rd party folder'
    PROTOC_BIN=`which protoc`
  else
    echo 'Error: protoc is missing from the 3rd party folder and on the PATH'
    exit 1
  fi
fi

set -x
for f in $PROTO_FILES ; do
  $PROTOC_BIN -I=$SRC_DIR --java_out=$JAVA_DIR $f
done
