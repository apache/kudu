#!/bin/bash
########################################################################
# Copyright (c) 2014 Cloudera, Inc.
#
# Run tpch benchmark and write results to the DB.
#
# Expects to find the following environment variables set:
# - JOB_NAME
# - BUILD_NUMBER
#
# Jenkins job: http://sandbox.jenkins.cloudera.com/job/kudu-tpch1
########################################################################

# Set up environment
set -e
ulimit -m $[3000*1000]
ulimit -c unlimited # gather core dumps

# PATH=<toolchain_stuff>:$PATH
export TOOLCHAIN=/mnt/toolchain/toolchain.sh
if [ -f "$TOOLCHAIN" ]; then
  source $TOOLCHAIN
fi

# Build thirdparty
thirdparty/build-if-necessary.sh

# PATH=<thirdparty_stuff>:<toolchain_stuff>:$PATH
export PATH=$BASE_DIR/thirdparty/installed/bin:$PATH
export PPROF_PATH=$BASE_DIR/thirdparty/installed/bin/pprof

# Build Kudu
rm -rf CMakeCache.txt CMakeFiles

BUILD_TYPE=release
# Workaround for gperftools issue #497
export LD_BIND_NOW=1

cmake . -DCMAKE_BUILD_TYPE=${BUILD_TYPE}
make clean
# clean up before we run
rm -Rf /tmp/kudutpch1-$UID
mkdir /tmp/kudutpch1-$UID

NUM_PROCS=$(cat /proc/cpuinfo | grep processor | wc -l)
make -j${NUM_PROCS} 2>&1 | tee build.log

export TBL_PATH=/home/jdcryans/lineitem.tbl
export SCRIPT_PATH=src/scripts/write-jobs-stats-to-mysql.py

# warming up the OS buffer
cat $TBL_PATH > /dev/null
cat $TBL_PATH > /dev/null

TPCH_OUTDIR=/data/2/tmp/kudutpch1-jenkins
rm -rf $TPCH_OUTDIR
./build/release/tpch1 -logtostderr=1 -tpch_path_to_data=$TBL_PATH -mini_cluster_base_dir=$TPCH_OUTDIR -tpch_num_query_iterations=5 &> benchmark.log

cat benchmark.log

INSERT_TIME=$(grep "Time spent loading" benchmark.log | cut -d " " -f 9 | cut -d "s" -f 1)
QUERY_TIME_1=$(grep "iteration # 1" benchmark.log | cut -d " " -f 14 | cut -d "s" -f 1)
QUERY_TIME_2=$(grep "iteration # 2" benchmark.log | cut -d " " -f 14 | cut -d "s" -f 1)
QUERY_TIME_3=$(grep "iteration # 3" benchmark.log | cut -d " " -f 14 | cut -d "s" -f 1)
QUERY_TIME_4=$(grep "iteration # 4" benchmark.log | cut -d " " -f 14 | cut -d "s" -f 1)

python $SCRIPT_PATH $JOB_NAME $BUILD_NUMBER insert_1gb 1 $INSERT_TIME

python $SCRIPT_PATH $JOB_NAME $BUILD_NUMBER query_1_1gb 1 $QUERY_TIME_1
python $SCRIPT_PATH $JOB_NAME $BUILD_NUMBER query_1_1gb 2 $QUERY_TIME_2
python $SCRIPT_PATH $JOB_NAME $BUILD_NUMBER query_1_1gb 3 $QUERY_TIME_3
python $SCRIPT_PATH $JOB_NAME $BUILD_NUMBER query_1_1gb 4 $QUERY_TIME_4