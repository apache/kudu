#!/bin/bash -e
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# Little script to parse the output from rpc-bench.


FILE=$1
if [[ -z $FILE || $FILE == "-h" || $FILE == "--help" ]]; then
  echo "Usage: $0 rpc-bench-output.log"
  echo
  echo 'Example:'
  echo
  echo '$ cd $KUDU_HOME'
  echo '$ BUILD_TYPE=RELEASE ./build-support/jenkins/build-and-test.sh'
  echo '$ KUDU_ALLOW_SLOW_TESTS=1 ./build/latest/rpc-bench --gtest_repeat=10 2>&1 | tee rpc-bench-output.log'
  echo '$ ./src/kudu/benchmarks/bin/parse_rpc_bench.sh rpc-bench-output.log'
  echo
  echo 'Example output:'
  echo
  echo 'Reqs/sec: runs=10, avg=146661.6, max=147649'
  echo 'User CPU per req: runs=10, avg=16.3004, max=16.4745'
  echo 'Sys CPU per req: runs=10, avg=29.25029, max=29.5273'
  echo
  exit 1
fi

# Just some hacky one-liners to parse and summarize the output files.
# Don't forget to redirect stderr to stdout when teeing the rpc-bench output to the log file!
perl -ne '/ (Reqs\/sec):\s+(\d+(?:\.(?:\d+)?)?)/ or next; $lab = $1; $m = $2 if $2 > $m; $v += $2; $ct++; END { print "$lab: runs=$ct, avg=" . $v/$ct . ", max=$m\n"; }' < $FILE
perl -ne '/ (User CPU per req):\s+(\d+(?:\.(?:\d+)?)?)/ or next; $lab = $1; $m = $2 if $2 > $m; $v += $2; $ct++; END { print "$lab: runs=$ct, avg=" . $v/$ct . ", max=$m\n"; }' < $FILE
perl -ne '/ (Sys CPU per req):\s+(\d+(?:\.(?:\d+)?)?)/ or next; $lab = $1; $m = $2 if $2 > $m; $v += $2; $ct++; END { print "$lab: runs=$ct, avg=" . $v/$ct . ", max=$m\n"; }' < $FILE
