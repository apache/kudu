#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script tests for KUDU-1508 in the local kernel's implementation of ext4.
# To use it, you must compile hole_punch_range.c and make it available in the
# current working directory.
#
# This script must either be run as root, or as a user with sudo.

# Configurable options.
BLOCK_DEVICE_FILE=block
MOUNT_DIR=mnt
TEST_FILE=test
BLOCK_SIZES="1024 2048 4096"

# Run a command given in the first argument.
#
# If the second argument is set and the command fails, the exit code is
# returned and the command output is squelched.
#
# If the second argument is unset and the command fails, the command output is
# emitted and the script exits.
function run() {
  local CMD="$1"
  local FAIL_OK="$2"
  local OUT RETVAL

  OUT=$($CMD 2>&1)
  RETVAL=$?
  if [ $RETVAL -ne 0 ]; then
    if [ -n "$FAIL_OK" ]; then
      return $RETVAL
    fi

    echo "Command failed: $CMD"
    echo "Result: $OUT"
    exit $RETVAL
  fi
}

# Run an iteration of the test in a local test-only filesystem.
#
# Returns whether the filesystem was corrupted.
function run_test() {
  local BLOCK_SIZE=$1
  local BLOCK_NUM=$2

  local FILESYSTEM_SIZE=$(($BLOCK_SIZE * $BLOCK_NUM * 4))
  local TEST_PATH=$MOUNT_DIR/$TEST_FILE
  local FSCK_RESULT
  local INTERIOR_NODE_LAST_BLOCK

  # Clean up in preparation.
  run "sudo umount $MOUNT_DIR" fail_ok
  run "mkdir -p $MOUNT_DIR"
  run "rm -f $BLOCK_DEVICE_FILE"

  # Create the test filesystem and file.
  #
  # The 'sync' at the end speeds up subsequent hole punching.
  run "fallocate --length $FILESYSTEM_SIZE $BLOCK_DEVICE_FILE"
  run "mkfs -F -t ext4 -b $BLOCK_SIZE $BLOCK_DEVICE_FILE"
  run "sudo mount -o loop $BLOCK_DEVICE_FILE $MOUNT_DIR"
  run "sudo chown $EUID $MOUNT_DIR"
  run "dd if=/dev/zero of=$TEST_PATH conv=notrunc bs=$BLOCK_SIZE count=$BLOCK_NUM"
  run "sync"

  # Maximize the number of extents in the file by punching holes in every other
  # block.
  #
  # The 'sync' at the end makes the extent tree visible to debugfs.
  run "./hole_punch_range $TEST_PATH 0 $BLOCK_SIZE 2"
  run "sync"

  # Determine the block number of the last block in the first level 1 interior
  # node in the file's extent tree.
  while read LINE; do
    # Find the node. Its debugfs line will look something like this:
    #
    #  1/ 2   1/340      1 -    680 163969             680
    #
    # The last number in the line is the number of blocks pointed to by the
    # node, and since we're looking at the first level 1 interior node, it's
    # also the last block number pointed to by that node.
    if echo $LINE | grep -q "[[:space:]]*1/[[:space:]]*[[:digit:]]*[[:space:]]*1/"; then
      INTERIOR_NODE_LAST_BLOCK=$(echo $LINE | awk '{ print $NF }')
      break
    fi
  done < <(debugfs -R "dump_extents -n $TEST_FILE" $BLOCK_DEVICE_FILE 2> /dev/null)

  # If we can't find the last block number, the file does not have an extent
  # tree with at least two interior levels, and the subsequent corruption test
  # can be skipped.
  if [ -n "$INTERIOR_NODE_LAST_BLOCK" ]; then
    # Try to corrupt the filesystem by punching out all of the remaining blocks
    # belonging to this interior node.
    #
    # If the filesystem is vulnerable to KUDU-1508, it'll fail to update the
    # interior node's parent, corrupting the filesystem until the next fsck.
    run "./hole_punch_range $TEST_PATH 1 $INTERIOR_NODE_LAST_BLOCK 2"
  fi

  run "sudo umount $MOUNT_DIR"

  # Test the filesystem for corruption and return the result.
  run "fsck.ext4 $BLOCK_DEVICE_FILE -f -n" fail_ok
  FSCK_RESULT=$?

  # Clean up after the test.
  run "rmdir $MOUNT_DIR"
  run "rm -f $BLOCK_DEVICE_FILE"

  return $FSCK_RESULT
}

# Run the test with known bad parameters to see if this kernel is worth testing.
run_test 4096 $((16 * 1024))
if [[ $? -eq 0 ]]; then
  echo "This kernel is not vulnerable to KUDU-1508, skipping test"
  exit 0
fi

echo "This kernel is vulnerable to KUDU-1508, finding per-block size upper bounds"

# Now figure out, for each block size, what the max number of blocks is.
for BLOCK_SIZE in $BLOCK_SIZES; do
  MIN=0
  MAX=$((1024 * 16))
  CUR=
  echo "Block size $BLOCK_SIZE: searching for block number upper bound (MIN=$MIN,MAX=$MAX)"
  while [[ $MIN -lt $MAX ]] ; do
    CUR=$((($MAX + $MIN) / 2))
    run_test $BLOCK_SIZE $CUR
    if [[ $? -eq 0 ]]; then
      echo "Block size $BLOCK_SIZE: $CUR good (MIN=$MIN,MAX=$MAX)"
      MIN=$(($CUR + 1))
    else
      echo "Block size $BLOCK_SIZE: $CUR bad (MIN=$MIN,MAX=$MAX)"
      MAX=$(($CUR - 1))
    fi
  done

  echo "Block size $BLOCK_SIZE: upper bound found at $CUR"
done
