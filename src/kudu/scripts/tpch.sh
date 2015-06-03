#!/bin/bash -xe
########################################################################
# Copyright (c) 2014 Cloudera, Inc.
# Confidential Cloudera Information: Covered by NDA.
#
# Run tpch benchmark and write results to the DB.
#
# Expects to find the following Jenkins environment variables set:
# - JOB_NAME
# - BUILD_NUMBER
# If these are not set, the script will still run but will not record
# the results in the MySQL database. Instead, it will output results
# into .tsv files in the kudu source root directory. This is useful for
# running this benchmark locally for testing / dev purposes.
#
# Optional environment variables to override (defaults set for Jenkins):
# - LINEITEM_TBL_PATH: Path to lineitem.tbl from the TPC-H suite.
# - KUDU_DATA_DIR: Directory to use for data storage.
# - TPCH_NUM_QUERY_ITERS: Number of TPC-H query iterations to run.
#
# Jenkins job: http://sandbox.jenkins.cloudera.com/job/kudu-tpch1
########################################################################

##########################################################
# Constants
##########################################################
ROOT=$(readlink -f $(dirname $0)/../../..)
OUTDIR=$ROOT/build/tpch

##########################################################
# Overridable params
##########################################################
LINEITEM_TBL_PATH=${LINEITEM_TBL_PATH:-/home/jdcryans/lineitem.tbl}
KUDU_DATA_DIR=${KUDU_DATA_DIR:-/data/2/tmp/kudutpch1-jenkins}
TPCH_NUM_QUERY_ITERS=${TPCH_NUM_QUERY_ITERS:-5}

##########################################################
# Functions
##########################################################
record_result() {
  local RECORD_STATS_SCRIPT=$ROOT/src/kudu/scripts/write-jobs-stats-to-mysql.py
  local TEST_NAME=$1
  local ITER=$2
  local VALUE=$3
  if [ -n "$JOB_NAME" ]; then
    # Jenkins.
    python $RECORD_STATS_SCRIPT $JOB_NAME $BUILD_NUMBER $TEST_NAME $ITER $VALUE
  else
    # Running locally.
    local STATS_FILE="$OUTDIR/tpch-$TEST_NAME.tsv"
    echo -e "${TEST_NAME}\t${ITER}\t${VALUE}" >> "$STATS_FILE"
  fi
}

##########################################################
# Main
##########################################################
if [ $TPCH_NUM_QUERY_ITERS -lt 2 ]; then
  echo "Error: TPCH_NUM_QUERY_ITERS must be 2 or greater"
  exit 1
fi

cd $ROOT

# Set up environment
set -o pipefail
ulimit -m $[3000*1000]
ulimit -c unlimited # gather core dumps

# PATH=<toolchain_stuff>:$PATH
export TOOLCHAIN=/mnt/toolchain/toolchain.sh
if [ -f "$TOOLCHAIN" ]; then
  source $TOOLCHAIN
fi

# Build thirdparty
$ROOT/thirdparty/build-if-necessary.sh

# PATH=<thirdparty_stuff>:<toolchain_stuff>:$PATH
export PATH=$(pwd)/thirdparty/installed/bin:$PATH
export PPROF_PATH=$(pwd)/thirdparty/installed/bin/pprof

# Build Kudu
rm -rf CMakeCache.txt CMakeFiles

BUILD_TYPE=release
# Workaround for gperftools issue #497
export LD_BIND_NOW=1

cmake . -DCMAKE_BUILD_TYPE=${BUILD_TYPE}
make clean

NUM_PROCS=$(cat /proc/cpuinfo | grep processor | wc -l)
make -j${NUM_PROCS} tpch1 2>&1 | tee build.log

# Warming up the OS buffer.
cat $LINEITEM_TBL_PATH > /dev/null
cat $LINEITEM_TBL_PATH > /dev/null

rm -Rf $KUDU_DATA_DIR   # Clean up data dir.
mkdir -p $OUTDIR        # Create log file output dir.

./build/release/tpch1 -logtostderr=1 \
                      -tpch_path_to_data=$LINEITEM_TBL_PATH \
                      -mini_cluster_base_dir=$KUDU_DATA_DIR \
                      -tpch_num_query_iterations=$TPCH_NUM_QUERY_ITERS \
                      >$OUTDIR/benchmark.log 2>&1

cat $OUTDIR/benchmark.log
INSERT_TIME=$(grep "Time spent loading" $OUTDIR/benchmark.log | \
    perl -pe 's/.*Time spent loading: real ([0-9\.]+)s.*/\1/')
record_result insert_1gb 1 $INSERT_TIME

# We do not record the first iteration (#0) because we want to record the
# in-cache performance.
for iter in $(seq 1 $(expr $TPCH_NUM_QUERY_ITERS - 1)); do
  QUERY_TIME=$(grep "iteration # $iter" $OUTDIR/benchmark.log | \
      perl -pe "s/.*iteration # $iter: real ([0-9\.]+)s.*/\1/")
  record_result query_1_1gb $iter $QUERY_TIME
done
