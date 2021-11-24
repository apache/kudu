#!/bin/bash -xe
########################################################################
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
#
# Run and compare benchmarks.
#
# Allows for running comparisons either locally or as part of a
# Jenkins job which integrates with a historical stats DB.
# Run this script with -help for usage information.
########################################################################

# Fail the job if any part fails, even when piping through 'tee', etc.
set -o pipefail

################################################################
# Constants
################################################################

MODE_JENKINS="jenkins"
MODE_LOCAL="local"

# In Jenkins mode, get last 10 weeks of stats stored in the database.
STATS_DAYS_TO_FETCH=70

LOCAL_STATS_BASE="local-stats"

NUM_MT_TABLET_TESTS=5
MT_TABLET_TEST=mt-tablet-test
RPC_BENCH_TEST=RpcBenchBenchmark
CBTREE_TEST=ConcurrentBTreeScan
BLOOM_TEST=BloomfileBenchmark
MT_BLOOM_TEST=MultithreadedBloomfileBenchmark
WIRE_PROTOCOL_TEST=WireProtocolBenchmark
COMPACT_MERGE_BENCH=CompactBenchMerge
WITH_OVERLAP=Overlap
NO_OVERLAP=NoOverlap

MEMROWSET_BENCH=MemRowSetBenchmark
TS_INSERT_LATENCY=TabletServerInsertLatency
TS_8THREAD_BENCH=TabletServer8Threads
INSERT=Insert
SCAN_NONE_COMMITTED=ScanNoneCommitted
SCAN_ALL_COMMITTED=ScanAllCommitted

FS_SCANINSERT_MRS=FullStackScanInsertMRSOnly
FS_SCANINSERT_DISK=FullStackScanInsertWithDisk

DENSE_NODE_ITEST=DenseNodeItest
BLOCKING_QUEUE_SYMMETRIC_TEST=BlockingQueueSymmetric
BLOCKING_QUEUE_NON_SYMMETRIC_TEST=BlockingQueueNonSymmetric

GET_TABLE_SCHEMA_RPC=GetTableSchemaTestRpc
GET_TABLE_SCHEMA_DIRECT_CALL=GetTableSchemaTestDirectCall

GET_TABLE_LOCATIONS_RPC=GetTableLocationsTestRpc
GET_TABLE_LOCATIONS_DIRECT_CALL=GetTableLocationsTestDirectCall

SAME_TABLET_CONCURRENT_WRITES=SameTabletConcurrentWrites

CLIENT_METACACHE_PERF=ClientMetacachePerf
CLIENT_METACACHE_PERF_SYNTHETIC=ClientMetacachePerfSynthetic

LOG_DIR_NAME=build/latest/bench-logs
OUT_DIR_NAME=build/latest/bench-out
HTML_FILE="benchmarks.html"

# Most tests will run this many times.
NUM_SAMPLES=${NUM_SAMPLES:-10}

# Whether to run the benchmarks under 'perf stat', recording and plotting
# the statistics collected by 'perf stat'.  The 'perf stat' has the
# least overhead among all sub-commands of the 'perf' suite (e.g., see
# http://www.brendangregg.com/perf.html).
RUN_PERF_STAT=${RUN_PERF_STAT:-1}
if [ $RUN_PERF_STAT -ne 0 -o "$RUN_PERF_STAT" = x"yes" ]; then
  PERF_STAT="perf stat"
else
  PERF_STAT=""
fi

################################################################
# Global variables
################################################################

BENCHMARK_MODE=$MODE_JENKINS # we default to "jenkins mode"
BASE_DIR=$(pwd)
LOGDIR=""
OUTDIR=""

################################################################
# Functions
################################################################

usage_and_die() {
  set +x
  echo "Usage: $0 [-local [git-hash-1 [git-hash-2 ...]]]"
  echo "       When -local is specified, perf of 1 or more git hashes are plotted."
  echo "       Otherwise, the script is run in 'Jenkins' mode and expects the"
  echo "       usual Jenkins environment variables to be defined, such as"
  echo "       BUILD_NUMBER and JOB_NAME."
  exit 1
}

perf_stat_enabled() {
  if [ -z "$PERF_STAT" ]; then
    return 1
  fi
  return 0
}

# Ensure the necessary performance tools are present.
check_tools() {
  if perf_stat_enabled; then
    # Check for 'perf' assuming the 'stat' sub-tool is always available.
    if ! which perf > /dev/null; then
      return 1
    fi
  fi
  return 0
}

ensure_cpu_scaling() {
  $BASE_DIR/src/kudu/scripts/ensure_cpu_scaling.sh "$@"
}

record_result() {
  local BUILD_IDENTIFIER=$1
  local TEST_NAME=$2
  local ITER=$3
  local VALUE=$4
  if [ $BENCHMARK_MODE = $MODE_JENKINS ]; then
    python write-jobs-stats-to-mysql.py $JOB_NAME $BUILD_IDENTIFIER $TEST_NAME $ITER $VALUE
  else
    local STATS_FILE="$OUTDIR/$LOCAL_STATS_BASE-$TEST_NAME.tsv"
    # Note: literal tabs in below string.
    echo "${TEST_NAME}	${VALUE}	${BUILD_IDENTIFIER}" >> "$STATS_FILE"
  fi
}

load_stats() {
  local TEST_NAME="$1"
  if [ "$BENCHMARK_MODE" = "$MODE_JENKINS" ]; then
    python get-job-stats-from-mysql.py $TEST_NAME $STATS_DAYS_TO_FETCH
  else
    # Convert MySQL wildcards to shell wildcards.
    local TEST_NAME=$(echo $TEST_NAME | perl -pe 's/%/*/g')
    local STATS_FILES=$(ls $OUTDIR/$LOCAL_STATS_BASE-$TEST_NAME.tsv)
    # Note: literal tabs in below string.
    echo "workload	runtime	build_number"
    for f in $STATS_FILES; do
      cat $f
    done
  fi
}

write_img_plot() {
  local input_file="$1"
  local test_name="$2"
  local y_axis_label="$3"
  # Rscript fails when there's only a header, so just skip
  if [ `wc -l $input_file | cut -d ' ' -f1` -gt 1 ]; then
    Rscript jobs_runtime.R "$input_file" "$test_name" "$y_axis_label"
  fi
}

write_mttablet_img_plots() {
  local INPUT_FILE=$1
  local TEST_NAME=$2
  xvfb-run Rscript mt-tablet-test-graph.R $INPUT_FILE $TEST_NAME
}

build_kudu() {
  # PATH=<toolchain_stuff>:$PATH
  export TOOLCHAIN=/mnt/toolchain/toolchain.sh
  if [ -f "$TOOLCHAIN" ]; then
    source $TOOLCHAIN
  fi

  # Build thirdparty
  $BASE_DIR/build-support/enable_devtoolset.sh thirdparty/build-if-necessary.sh

  # PATH=<thirdparty_stuff>:<toolchain_stuff>:$PATH
  THIRDPARTY_BIN=$BASE_DIR/thirdparty/installed/common/bin
  export PPROF_PATH=$THIRDPARTY_BIN/pprof

  BUILD_TYPE=release

  # Build Kudu
  mkdir -p build/$BUILD_TYPE
  pushd build/$BUILD_TYPE
  rm -rf CMakeCache.txt CMakeFiles/

  $BASE_DIR/build-support/enable_devtoolset.sh $THIRDPARTY_BIN/cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ../..

  # clean up before we run
  rm -Rf /tmp/kudutpch1-$UID
  mkdir -p /tmp/kudutpch1-$UID

  NUM_PROCS=$(cat /proc/cpuinfo | grep processor | wc -l)
  make -j${NUM_PROCS} 2>&1 | tee build.log
  popd

}

run_benchmarks() {
  # Create output directories if needed.
  mkdir -p "$LOGDIR"
  mkdir -p "$OUTDIR"

  export KUDU_ALLOW_SLOW_TESTS=true

  # run all of the variations of mt-tablet-test
  $PERF_STAT ./build/latest/bin/mt-tablet-test \
    --gtest_filter=\*DoTestAllAtOnce\* \
    --num_counter_threads=0 \
    --tablet_test_flush_threshold_mb=32 \
    --num_slowreader_threads=0 \
    --flusher_backoff=1.0 \
    --flusher_initial_frequency_ms=1000 \
    --inserts_per_thread=1000000 \
    &> $LOGDIR/${MT_TABLET_TEST}.log

  # run rpc-bench test 5 times. 10 seconds per run
  for i in $(seq 1 $NUM_SAMPLES); do
    $PERF_STAT ./build/latest/bin/rpc-bench \
      --gtest_filter=*BenchmarkCalls \
      &> $LOGDIR/$RPC_BENCH_TEST$i.log
  done

  # run cbtree-test 5 times. 20 seconds per run
  for i in $(seq 1 $NUM_SAMPLES); do
    $PERF_STAT ./build/latest/bin/cbtree-test \
      --gtest_filter=TestCBTree.TestScanPerformance \
      &> $LOGDIR/${CBTREE_TEST}$i.log
  done

  # run bloomfile-test 5 times. ~3.3 seconds per run
  for i in $(seq 1 $NUM_SAMPLES); do
    $PERF_STAT ./build/latest/bin/bloomfile-test \
      --gtest_filter=*Benchmark \
      --benchmark_queries=10000000 \
      --bloom_size_bytes=32768 \
      --n_keys=100000 \
      &> $LOGDIR/$BLOOM_TEST$i.log
  done

  # run mt-bloomfile-test 5 times. 20-30 seconds per run.
  # We use 150M keys so the total bloom space is >150MB, and then
  # set the cache capacity to be only 100M to ensure that the cache
  # churns a lot.
  for i in $(seq 1 $NUM_SAMPLES); do
    $PERF_STAT ./build/latest/bin/mt-bloomfile-test \
      --benchmark_queries=2000000 --bloom_size_bytes=4096 \
      --n_keys=150000000 --block_cache_capacity_mb=100 \
      &> $LOGDIR/$MT_BLOOM_TEST$i.log
  done

  # run wire_protocol-test 5 times.
  #
  # We run the non-null 10-column benchmark, selecting all rows.
  for i in $(seq 1 $NUM_SAMPLES); do
    $PERF_STAT ./build/latest/bin/wire_protocol-test \
      --gtest_filter=*Benchmark/10_int64_non_null_sel_100pct \
      &> $LOGDIR/$WIRE_PROTOCOL_TEST$i.log
  done

  # run compaction-test 5 times, 6 seconds each
  for i in $(seq 1 $NUM_SAMPLES); do
    $PERF_STAT ./build/latest/bin/compaction-test \
      --gtest_filter=TestCompaction.BenchmarkMerge* \
      &> $LOGDIR/${COMPACT_MERGE_BENCH}$i.log
  done

  # run memrowset benchmark 5 times, ~10 seconds per run
  for i in $(seq 1 $NUM_SAMPLES) ; do
    $PERF_STAT ./build/latest/bin/memrowset-test \
      --roundtrip_num_rows=10000000 \
      --gtest_filter=\*InsertCount\* \
      &> $LOGDIR/${MEMROWSET_BENCH}$i.log
  done

  # Run single-threaded TS insert latency benchmark, 5-6 seconds per run
  for i in $(seq 1 $NUM_SAMPLES) ; do
    $PERF_STAT ./build/latest/bin/tablet_server-test \
      --gtest_filter=*MicroBench* \
      --single_threaded_insert_latency_bench_warmup_rows=1000 \
      --single_threaded_insert_latency_bench_insert_rows=10000 \
      &> $LOGDIR/${TS_INSERT_LATENCY}$i.log
  done

  # Run multi-threaded TS insert benchmark
  for i in $(seq 1 $NUM_SAMPLES) ; do
    $PERF_STAT ./build/latest/bin/tablet_server-stress-test \
      --num_inserts_per_thread=30000 -runtime_secs=0 \
      &> $LOGDIR/${TS_8THREAD_BENCH}$i.log
  done

  # Run full stack scan/insert test using MRS only, ~26s each
  for i in $(seq 1 $NUM_SAMPLES) ; do
    # TODO(aserbin): fix --perf_stat_scan and enable stats collection
    ./build/latest/bin/full_stack-insert-scan-test \
      --gtest_filter=FullStackInsertScanTest.MRSOnlyStressTest \
      --concurrent_inserts=50 \
      --inserts_per_client=200000 \
      --rows_per_batch=10000 \
      &> $LOGDIR/${FS_SCANINSERT_MRS}$i.log
  done

  # Run full stack scan/insert test with disk, ~50s each
  for i in $(seq 1 $NUM_SAMPLES) ; do
    # TODO(aserbin): fix --perf_stat_scan and enable stats collection
    ./build/latest/bin/full_stack-insert-scan-test \
      --gtest_filter=FullStackInsertScanTest.WithDiskStressTest \
      --concurrent_inserts=50 \
      --inserts_per_client=200000 \
      --rows_per_batch=10000 \
      &> $LOGDIR/${FS_SCANINSERT_DISK}$i.log
  done

  # Run dense storage node test, ~150s each.
  #
  # Needs to run as root for measure_startup_drop_caches.
  # TODO(aserbin): add performance counter collection once it's possible to run
  #                external minicluster's components under 'perf stat'
  for i in $(seq 1 $NUM_SAMPLES) ; do
    sudo ./build/latest/bin/dense_node-itest \
      --num_seconds=60 \
      --num_tablets=1000 \
      --measure_startup_sync \
      --measure_startup_drop_caches \
      --measure_startup_wait_for_bootstrap \
      &> $LOGDIR/${DENSE_NODE_ITEST}$i.log
  done

  # Run BlockingQueue concurrency test with 3 writers and 3 readers,
  # (i.e. symmetric in number of readers/writers), no non-blocking writers.
  for i in $(seq 1 $NUM_SAMPLES) ; do
    $PERF_STAT ./build/latest/bin/blocking_queue-test \
      --gtest_filter=BlockingQueueMultiThreadPerfTest.RequestRates \
      --num_blocking_writers=3 \
      --num_blocking_readers=3 \
      --num_non_blocking_writers=0 \
      &> $LOGDIR/${BLOCKING_QUEUE_SYMMETRIC_TEST}$i.log
  done

  # Run BlockingQueue concurrency test with 3 writers
  # (2 blocking, 1 non-blocking) and 1 reader.
  for i in $(seq 1 $NUM_SAMPLES) ; do
    $PERF_STAT ./build/latest/bin/blocking_queue-test \
      --gtest_filter=BlockingQueueMultiThreadPerfTest.RequestRates \
      --num_blocking_writers=2 \
      --num_blocking_readers=1 \
      --num_non_blocking_writers=1 \
      &> $LOGDIR/${BLOCKING_QUEUE_NON_SYMMETRIC_TEST}$i.log
  done

  # Run ConcurrentGetTableSchemaTest.RPC with and without authz.
  for i in $(seq 1 $NUM_SAMPLES) ; do
    $PERF_STAT ./build/latest/bin/master-test \
      --gtest_filter='*/ConcurrentGetTableSchemaTest.Rpc/*' \
      &> $LOGDIR/${GET_TABLE_SCHEMA_RPC}$i.log
  done

  # Run ConcurrentGetTableSchemaTest.DirectMethodCall with and without authz.
  for i in $(seq 1 $NUM_SAMPLES) ; do
    $PERF_STAT ./build/latest/bin/master-test \
      --gtest_filter='*/ConcurrentGetTableSchemaTest.DirectMethodCall/*' \
      &> $LOGDIR/${GET_TABLE_SCHEMA_DIRECT_CALL}$i.log
  done

  # Run GetTableLocationsBenchmark, with and without cache.
  for capacity_mb in 0 32 ; do
    for i in $(seq 1 $NUM_SAMPLES) ; do
      $PERF_STAT ./build/latest/bin/table_locations-itest \
        --gtest_filter=TableLocationsTest.GetTableLocationsBenchmark \
        --rpc_num_service_threads=8 \
        --benchmark_num_threads=12 \
        --table_locations_cache_capacity_mb=$capacity_mb \
        &> $LOGDIR/${GET_TABLE_LOCATIONS_RPC}_${capacity_mb}$i.log
    done
  done

  # Run GetTableLocationsBenchmarkFunctionCall, with and without cache.
  for capacity_mb in 0 32 ; do
    for i in $(seq 1 $NUM_SAMPLES) ; do
      $PERF_STAT ./build/latest/bin/table_locations-itest \
        --gtest_filter=TableLocationsTest.GetTableLocationsBenchmarkFunctionCall \
        --benchmark_num_threads=12 \
        --table_locations_cache_capacity_mb=$capacity_mb \
        &> $LOGDIR/${GET_TABLE_LOCATIONS_DIRECT_CALL}_${capacity_mb}$i.log
    done
  done

  # Run SameTabletConcurrentWritesTest.InsertsOnly with 16 inserter threads.
  for i in $(seq 1 $NUM_SAMPLES) ; do
    $PERF_STAT ./build/latest/bin/same_tablet_concurrent_writes-itest \
      --gtest_filter='SameTabletConcurrentWritesTest.InsertsOnly' \
      --num_inserter_threads=16 \
      &> $LOGDIR/${SAME_TABLET_CONCURRENT_WRITES}$i.log
  done

  # Run MetaCacheLookupStressTest.Perf scenario.
  for i in $(seq 1 $NUM_SAMPLES) ; do
    $PERF_STAT ./build/latest/bin/client-stress-test \
      --gtest_filter='MetaCacheLookupStressTest.Perf' \
      &> $LOGDIR/${CLIENT_METACACHE_PERF}$i.log
  done

  # Run MetaCacheLookupStressTest.PerfSynthetic scenario.
  for i in $(seq 1 $NUM_SAMPLES) ; do
    $PERF_STAT ./build/latest/bin/client-stress-test \
      --gtest_filter='MetaCacheLookupStressTest.PerfSynthetic' \
      &> $LOGDIR/${CLIENT_METACACHE_PERF_SYNTHETIC}$i.log
  done
}

parse_and_record_perf_stats() {
  if ! perf_stat_enabled; then
    return
  fi
  local build_id=$1
  local test_id=$2
  local iter=$3
  local log=$4

  local ins_per_cycle=$(grep -o '# .* ins.* per cycle' $log | awk '{print $2}')
  record_result $build_id perf_stat_ins_per_cycle_${test_id} $iter $ins_per_cycle
  local cpu_util=$(grep -o '# .* CPUs utilized' $log | awk '{print $2}')
  record_result $build_id perf_stat_cpu_util_${test_id} $iter $cpu_util
  local branch_misses=$(grep -o '# .* of all branches' $log | awk '{print $2}' | sed 's/%//g')
  record_result $build_id perf_stat_branch_misses_${test_id} $iter $branch_misses
}

parse_and_record_all_results() {
  local BUILD_IDENTIFIER="$1"

  if [ -z "$BUILD_IDENTIFIER" ]; then
    echo "ERROR: BUILD_IDENTIFIER not defined"
    exit 1
  fi

  pushd src
  pushd kudu
  pushd scripts

  # parse the number of ms out of "[       OK ] MultiThreadedTabletTest/5.DoTestAllAtOnce (14966 ms)"
  local MT_TABLET_TEST_TIMINGS="${MT_TABLET_TEST}-timings"
  grep OK $LOGDIR/${MT_TABLET_TEST}.log | cut -d "(" -f2 | cut -d ")" -f1 | cut -d " " -f1 \
    > $LOGDIR/${MT_TABLET_TEST_TIMINGS}.txt

  # The tests go from 0 to NUM_MT_TABLET_TEST, but files start at line one so we add +1 to the line number.
  # Then using the timing we found, we multiply it by 1000 to gets seconds in float, then send it to MySQL
  for i in $(seq 0 $NUM_MT_TABLET_TESTS); do
    linenumber=$[ $i + 1 ]
    timing=`sed -n "${linenumber}p" $LOGDIR/${MT_TABLET_TEST_TIMINGS}.txt`
    record_result $BUILD_IDENTIFIER MultiThreadedTabletTest_$i 1 `echo $timing / 1000 | bc -l`
  done
  parse_and_record_perf_stats $BUILD_IDENTIFIER $MT_TABLET_TEST 1 $LOGDIR/${MT_TABLET_TEST}.log

  # parse out the real time from: "Time spent Insert 10000000 keys: real 16.438s user 16.164s  sys 0.229s"
  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$CBTREE_TEST
    local log=$LOGDIR/${id}${i}.log
    parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log

    real=`grep "Time spent Insert" $log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ${id}Insert $i $real

    real=`grep "not frozen" $log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ${id}NotFrozen $i $real

    real=`grep "(frozen" $log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ${id}Frozen $i $real
  done

  # parse out the real time from "Time spent with overlap: real 0.557s user 0.546s sys 0.010s"
  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$COMPACT_MERGE_BENCH
    local log=$LOGDIR/${id}${i}.log
    parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log

    real=`grep "with overlap" $log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ${id}${WITH_OVERLAP} $i $real

    real=`grep "without overlap" $log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ${id}${NO_OVERLAP} $i $real
  done

  # parse out time from MRS benchmarks
  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$MEMROWSET_BENCH
    local log=$LOGDIR/${id}${i}.log
    parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log

    real=`grep "Time spent Inserting" $log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ${id}${INSERT} $i $real
    real=`grep "Time spent Scanning rows where none" $log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ${id}${SCAN_NONE_COMMITTED} $i $real
    real=`grep "Time spent Scanning rows where all" $log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ${id}${SCAN_ALL_COMMITTED} $i $real
  done

  # Parse out the real time from: "Time spent Running 10000000 queries: real 3.281s  user 3.273s sys 0.000s"
  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$BLOOM_TEST
    local log=$LOGDIR/${id}${i}.log
    local real=`grep "Time spent Running" $log | ./parse_real_out.sh`
    parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log
    record_result $BUILD_IDENTIFIER $id $i $real
  done

  # Parse out the real time from: "Time spent Running 2000000 queries: real 28.193s user 26.903s sys 1.032s"
  # Many threads output their value, we keep the last;
  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$MT_BLOOM_TEST
    local log=$LOGDIR/${id}${i}.log
    local real=`grep "Time spent Running" $log | tail -n1 | ./parse_real_out.sh`
    parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log
    record_result $BUILD_IDENTIFIER $id $i $real
  done

  # Parse out row-wise rate of cycles/cell from:
  #   "Converting 10_int64_non_null to PB (method row-wise) row select rate 1: 30.196263 cycles/cell"
  # Parse out the columnar rate of cycles/cell from:
  #   "Converting 10_int64_non_null to PB (method columnar) row select rate 1: 1.313369 cycles/cell"
  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$WIRE_PROTOCOL_TEST
    local log=$LOGDIR/${id}${i}.log
    parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log

    local real_rowwise=`grep "Converting.*to PB (method row-wise)" $log | sed 's|^.*: \([[:digit:].]*\) cycles/cell$|\1|'`
    record_result $BUILD_IDENTIFIER $id $i $real_rowwise

    local real_colwise=`grep "Converting.*to PB (method columnar)" $log | sed 's|^.*: \([[:digit:].]*\) cycles/cell$|\1|'`
    record_result $BUILD_IDENTIFIER ${id}_columnar $i $real_colwise
  done

  # parse the rate out of: "I1009 15:00:30.023576 27043 rpc-bench.cc:108] Reqs/sec:         84404.4"
  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$RPC_BENCH_TEST
    local log=$LOGDIR/${id}${i}.log
    local rate=`grep Reqs $log | cut -d ":" -f 5 | tr -d ' '`
    parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log
    record_result $BUILD_IDENTIFIER $id $i $rate
  done

  # parse latency numbers from single-threaded tserver benchmark
  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$TS_INSERT_LATENCY
    local log=$LOGDIR/${id}${i}.log
    parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log
    for metric in min mean percentile_95 percentile_99 percentile_99_9 ; do
      val=$(grep "\"$metric\": " $log | awk '{print $2}' | sed -e 's/,//')
      record_result $BUILD_IDENTIFIER ${id}_${metric} $i $val
    done
  done

  # parse latency and throughput numbers from multi-threaded tserver benchmark
  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$TS_8THREAD_BENCH
    local log=$LOGDIR/${id}${i}.log
    parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log
    for metric in min mean percentile_95 percentile_99 percentile_99_9 ; do
      val=$(grep "\"$metric\": " $log | awk '{print $2}' | sed -e 's/,//')
      record_result $BUILD_IDENTIFIER ${id}_${metric}_latency $i $val
    done
    rate=$(grep -o 'Throughput.*' $log | awk '{print $2}')
    record_result $BUILD_IDENTIFIER ${id}_throughput $i $rate
    rate=$(grep -o 'CPU efficiency.*' $log | awk '{print $3}')
    record_result $BUILD_IDENTIFIER ${id}_cpu_efficiency $i $rate
  done

  # parse scan timings for scans and inserts with MRS only
  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$FS_SCANINSERT_MRS
    local log=$LOGDIR/${id}${i}.log
    insert=`grep "Time spent concurrent inserts" $log | ./parse_real_out.sh`
    scan_full=`grep "Time spent full schema scan" $log | ./parse_real_out.sh`
    scan_str=`grep "Time spent String projection" $log | ./parse_real_out.sh`
    scan_int32=`grep "Time spent Int32 projection" $log | ./parse_real_out.sh`
    scan_int64=`grep "Time spent Int64 projection" $log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ${id}_insert $i $insert
    record_result $BUILD_IDENTIFIER ${id}_scan_full $i $scan_full
    record_result $BUILD_IDENTIFIER ${id}_scan_str $i $scan_str
    record_result $BUILD_IDENTIFIER ${id}_scan_int32 $i $scan_int32
    record_result $BUILD_IDENTIFIER ${id}_scan_int64 $i $scan_int64
  done

  # parse scan timings for scans and inserts with disk
  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$FS_SCANINSERT_DISK
    local log=$LOGDIR/${id}${i}.log
    insert=`grep "Time spent concurrent inserts" $log | ./parse_real_out.sh`
    scan_full=`grep "Time spent full schema scan" $log | ./parse_real_out.sh`
    scan_str=`grep "Time spent String projection" $log | ./parse_real_out.sh`
    scan_int32=`grep "Time spent Int32 projection" $log | ./parse_real_out.sh`
    scan_int64=`grep "Time spent Int64 projection" $log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ${id}_insert $i $insert
    record_result $BUILD_IDENTIFIER ${id}_scan_full $i $scan_full
    record_result $BUILD_IDENTIFIER ${id}_scan_str $i $scan_str
    record_result $BUILD_IDENTIFIER ${id}_scan_int32 $i $scan_int32
    record_result $BUILD_IDENTIFIER ${id}_scan_int64 $i $scan_int64
  done

  # Parse timings and thread count
  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$DENSE_NODE_ITEST
    local log=$LOGDIR/${id}${i}.log
    num_threads=`grep threads_running $log | cut -d ":" -f 5 | tr -d ' '`
    num_blocks=`grep log_block_manager_blocks_under_management $log | cut -d ":" -f 5 | tr -d ' '`
    num_bytes=`grep log_block_manager_bytes_under_management $log | cut -d ":" -f 5 | tr -d ' '`
    num_containers=`grep log_block_manager_containers $log | cut -d ":" -f 5 | tr -d ' '`
    num_full_containers=`grep log_block_manager_full_containers $log | cut -d ":" -f 5 | tr -d ' '`

    # For some inexplicable reason, these timing measurements are sometimes
    # not emitted to the log file. Having spent a great deal of time trying to
    # figure out why, let's just skip missing measurements.
    #
    # This may cause resulting graphs to look a little sharp, but that's better
    # than failing the entire script.
    set +e
    time_restarting_tserver=`grep "Time spent restarting tserver" $log | ./parse_real_out.sh`
    time_bootstrapping_tablets=`grep "Time spent bootstrapping tablets" $log | ./parse_real_out.sh`
    set -e
    record_result $BUILD_IDENTIFIER ${id}_num_threads $i $num_threads
    record_result $BUILD_IDENTIFIER ${id}_num_blocks $i $num_blocks
    record_result $BUILD_IDENTIFIER ${id}_num_bytes $i $num_bytes
    record_result $BUILD_IDENTIFIER ${id}_num_containers $i $num_containers
    record_result $BUILD_IDENTIFIER ${id}_num_full_containers $i $num_full_containers
    if [ -n "$time_restarting_tserver" ]; then
      record_result $BUILD_IDENTIFIER ${id}_time_restarting_tserver $i $time_restarting_tserver
    fi
    if [ -n "$time_bootstrapping_tablets" ]; then
      record_result $BUILD_IDENTIFIER ${id}_time_bootstrapping_tablets $i $time_bootstrapping_tablets
    fi

  done

  # Parse out total call rate and record the results for BlockingQueue test.
  for id in $BLOCKING_QUEUE_SYMMETRIC_TEST $BLOCKING_QUEUE_NON_SYMMETRIC_TEST; do
    for i in $(seq 1 $NUM_SAMPLES); do
      local log=$LOGDIR/${id}${i}.log
      parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log
      local rate=$(grep -o 'total rate: .* calls/sec' $log | awk '{print $3}')
      record_result $BUILD_IDENTIFIER ${id}_total_call_rate $i $rate
    done
  done

  # Parse out request rate and record the results for ConcurrentGetTableSchemaTest test.
  for id in $GET_TABLE_SCHEMA_RPC $GET_TABLE_SCHEMA_DIRECT_CALL; do
    for i in $(seq 1 $NUM_SAMPLES); do
      local log=$LOGDIR/${id}${i}.log
      parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log
      rate=$(grep -o 'GetTableSchema .*: .* req/sec (authz disabled)' $log | awk '{print $3}')
      record_result $BUILD_IDENTIFIER ${id}_no_authz_req_rate $i $rate
      rate=$(grep -o 'GetTableSchema .*: .* req/sec (authz enabled)' $log | awk '{print $3}')
      record_result $BUILD_IDENTIFIER ${id}_authz_req_rate $i $rate
    done
  done

  # Parse out request rate and record the results for GetTableLocationsBenchmark{FunctionCall}.
  for capacity_mb in 0 32 ; do
    for i in $(seq 1 $NUM_SAMPLES); do
      local id=${GET_TABLE_LOCATIONS_RPC}_${capacity_mb}
      local log=$LOGDIR/${id}${i}.log
      parse_and_record_perf_stats $BUILD_IDENTIFIER ${id} $i $log
      rate=$(grep -o 'GetTableLocations RPC: .* req/sec' $log | awk '{print $3}')
      record_result $BUILD_IDENTIFIER ${id}_req_rate $i $rate

      local id=${GET_TABLE_LOCATIONS_DIRECT_CALL}_${capacity_mb}
      local log=$LOGDIR/${id}${i}.log
      parse_and_record_perf_stats $BUILD_IDENTIFIER ${id} $i $log
      rate=$(grep -o 'GetTableLocations function call: .* req/sec' $log | awk '{print $4}')
      record_result $BUILD_IDENTIFIER ${id}_req_rate $i $rate
    done
  done

  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$SAME_TABLET_CONCURRENT_WRITES
    local log=$LOGDIR/${id}${i}.log
    parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log

    rate=$(grep -o 'write RPC request rate: .* req/sec' $log | awk '{print $5}')
    record_result $BUILD_IDENTIFIER ${id}_req_rate $i $rate
    overflows=$(grep -o 'total count of RPC queue overflows: .*' $log | awk '{print $7}')
    record_result $BUILD_IDENTIFIER ${id}_overflows $i $overflows
  done

  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$CLIENT_METACACHE_PERF
    local log=$LOGDIR/${id}${i}.log
    parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log

    local time_per_row=$(grep -o 'Time per row: .* ms' $log | awk '{print $4}')
    record_result $BUILD_IDENTIFIER ${id}_time_per_row $i $time_per_row
  done

  for i in $(seq 1 $NUM_SAMPLES); do
    local id=$CLIENT_METACACHE_PERF_SYNTHETIC
    local log=$LOGDIR/${id}${i}.log
    parse_and_record_perf_stats $BUILD_IDENTIFIER $id $i $log

    local time_per_row=$(grep -o 'Time per row: .* ms' $log | awk '{print $4}')
    record_result $BUILD_IDENTIFIER ${id}_time_per_row $i $time_per_row
  done

  popd
  popd
  popd
}

generate_ycsb_plots() {
  local WORKLOAD=$1
  local PHASE=$2
  METRIC_NAME=ycsb-$PHASE-$WORKLOAD

  # first plot the overall stats for that phase
  OVERALL_FILENAME=$METRIC_NAME-OVERALL
  load_stats $OVERALL_FILENAME-runtime_ms > $OUTDIR/$OVERALL_FILENAME-runtime_ms.tsv
  write_img_plot $OUTDIR/$OVERALL_FILENAME-runtime_ms.tsv $OVERALL_FILENAME-runtime_ms
  load_stats $OVERALL_FILENAME-throughput_ops_sec > $OUTDIR/$OVERALL_FILENAME-throughput_ops_sec.tsv
  write_img_plot $OUTDIR/$OVERALL_FILENAME-throughput_ops_sec.tsv $OVERALL_FILENAME-throughput_ops_sec

  # now plot the individual operations
  OPS="INSERT UPDATE READ"

  for op in $OPS; do
    OP_FILENAME=$METRIC_NAME-$op
    load_stats $OP_FILENAME-average_latency_us > $OUTDIR/$OP_FILENAME-average_latency_us.tsv
    write_img_plot $OUTDIR/$OP_FILENAME-average_latency_us.tsv $OP_FILENAME-average_latency_us

    load_stats $OP_FILENAME-95th_latency_ms > $OUTDIR/$OP_FILENAME-95th_latency_ms.tsv
    write_img_plot $OUTDIR/$OP_FILENAME-95th_latency_ms.tsv $OP_FILENAME-95th_latency_ms

    load_stats $OP_FILENAME-99th_latency_ms > $OUTDIR/$OP_FILENAME-99th_latency_ms.tsv
    write_img_plot $OUTDIR/$OP_FILENAME-99th_latency_ms.tsv $OP_FILENAME-99th_latency_ms
  done
}

load_and_generate_plot() {
  local test_name=$1
  local plot_name=$2
  local y_axis_label=$3
  if [ -z "$y_axis_label" ]; then
    local y_axis_label="runtime (sec)"
  fi
  local plot_file=${OUTDIR}/${plot_name}.tsv
  load_stats "$test_name" > $plot_file
  write_img_plot $plot_file $plot_name "$y_axis_label"
}

load_and_generate_perf_stat_plots() {
  local id="perf_stat_ins_per_cycle"
  local plot_file=${OUTDIR}/${id}.tsv
  load_stats "${id}_%" > $plot_file
  write_img_plot $plot_file "perf-stat-instructions-per-CPU-cycle" "instructions per cycle"

  local id="perf_stat_cpu_util"
  local plot_file=${OUTDIR}/${id}.tsv
  load_stats "${id}_%" > $plot_file
  write_img_plot $plot_file "perf-stat-cpu-utilization" "CPUs utilized"

  local id="perf_stat_branch_misses"
  local plot_file=${OUTDIR}/${id}.tsv
  load_stats "${id}_%" > $plot_file
  write_img_plot $plot_file "perf-stat-branch-misses" "% of all branches"
}

load_stats_and_generate_plots() {
  pushd src
  pushd kudu
  pushd scripts

  load_and_generate_plot "%MultiThreadedTabletTest%" mt-tablet-test-runtime

  load_and_generate_plot ${CBTREE_TEST}Insert cb-tree-insert
  load_and_generate_plot ${CBTREE_TEST}NotFrozen cb-ctree-not-frozen
  load_and_generate_plot ${CBTREE_TEST}Frozen cb-ctree-frozen

  load_and_generate_plot "${COMPACT_MERGE_BENCH}%" compact-merge-bench

  load_and_generate_plot "${MEMROWSET_BENCH}${INSERT}" memrowset-bench-insert
  load_and_generate_plot "${MEMROWSET_BENCH}Scan%" memrowset-bench-scan

  load_and_generate_plot "$BLOOM_TEST" bloom-test
  load_and_generate_plot "$MT_BLOOM_TEST" mt-bloom-test

  load_and_generate_plot "${WIRE_PROTOCOL_TEST}" wire-protocol-test "conversion rate (cycle/cell)"
  load_and_generate_plot "${WIRE_PROTOCOL_TEST}_columnar" wire-protocol-test-columnar "conversion rate (cycle/cell)"

  load_and_generate_plot "$RPC_BENCH_TEST" rpc-bench-test "request rate (RPC/sec)"

  load_and_generate_plot "${TS_INSERT_LATENCY}%" ts-insert-latency "latency (usec)"

  load_and_generate_plot "${TS_8THREAD_BENCH}%_latency" ts-8thread-insert-latency "latency (usec)"
  load_and_generate_plot "${TS_8THREAD_BENCH}_throughput" ts-8thread-insert-throughput "throughput (row/sec)"
  load_and_generate_plot "${TS_8THREAD_BENCH}_cpu_efficiency" ts-8thread-insert-cpu-efficiency "CPU efficiency (row/cpu-sec)"

  load_and_generate_plot "${FS_SCANINSERT_MRS}%_insert" fs-mrsonly-insert
  load_and_generate_plot "${FS_SCANINSERT_MRS}%_scan%" fs-mrsonly-scan
  load_and_generate_plot "${FS_SCANINSERT_DISK}%_insert" fs-withdisk-insert
  load_and_generate_plot "${FS_SCANINSERT_DISK}%_scan%" fs-withdisk-scan

  load_and_generate_plot "${DENSE_NODE_ITEST}_time%" dense-node-bench-times
  load_and_generate_plot "${DENSE_NODE_ITEST}_num%containers%" dense-node-bench-containers "number of log block containers"
  load_and_generate_plot "${DENSE_NODE_ITEST}_num_blocks%" dense-node-bench-blocks "number of blocks"
  load_and_generate_plot "${DENSE_NODE_ITEST}_num_threads%" dense-node-bench-threads "threads running"
  load_and_generate_plot "${DENSE_NODE_ITEST}_num_bytes%" dense-node-bench-bytes "bytes under management"

  load_and_generate_plot "${BLOCKING_QUEUE_SYMMETRIC_TEST}%" blocking-queue-symmetric "function call rate (req/sec)"
  load_and_generate_plot "${BLOCKING_QUEUE_NON_SYMMETRIC_TEST}%" blocking-queue-non-symmetric "function call rate (req/sec)"

  load_and_generate_plot "${GET_TABLE_SCHEMA_RPC}%_req_rate" get-table-schema-rpc "RPC rate (req/sec)"
  load_and_generate_plot "${GET_TABLE_SCHEMA_DIRECT_CALL}_authz_req_rate" get-table-schema-dc-authz "function call rate (req/sec)"
  load_and_generate_plot "${GET_TABLE_SCHEMA_DIRECT_CALL}_no_authz_req_rate" get-table-schema-dc-no-authz "function call rate (req/sec)"

  load_and_generate_plot "${GET_TABLE_LOCATIONS_RPC}%_req_rate" get-table-locations-rpc
  load_and_generate_plot "${GET_TABLE_LOCATIONS_DIRECT_CALL}%_req_rate" get-table-locations-dc

  load_and_generate_plot "${SAME_TABLET_CONCURRENT_WRITES}_req_rate" same-tablet-concurrent-writes-rate "RPC rate (req/sec)"
  load_and_generate_plot "${SAME_TABLET_CONCURRENT_WRITES}_overflows" same-tablet-concurrent-writes-overflows "RPC queue overflows (total count)"

  load_and_generate_plot "${CLIENT_METACACHE_PERF}_time_per_row" client-metacache-lookup-perf "per-row apply time (sec)"
  load_and_generate_plot "${CLIENT_METACACHE_PERF_SYNTHETIC}_time_per_row" client-metacache-lookup-perf-synthetic "per-row lookup time (sec)"

  # Generate all the pngs for all the mt-tablet tests
  for i in $(seq 0 $NUM_MT_TABLET_TESTS); do
    cat $LOGDIR/${MT_TABLET_TEST}.log | ./graph-metrics.py MultiThreadedTabletTest/$i > $OUTDIR/test$i.tsv
    # Don't bail on failure (why not?)
    write_mttablet_img_plots $OUTDIR/test$i.tsv test$i || true
  done

  # Generate performance counters plots for all tests run under 'perf stat'.
  load_and_generate_perf_stat_plots

  if [ "${BENCHMARK_MODE}" = "${MODE_JENKINS}" ]; then
    ################################################################
    # Plot the separately-recorded TPCH and YCSB graphs as well
    # (only for Jenkins)
    ################################################################

    # TPC-H 1 runs separately, let's just get those graphs
    load_and_generate_plot query_1_1gb tpch1-query
    load_and_generate_plot insert_1gb tpch1-insert

    # YCSB which runs the 5nodes_workload on a cluster
    # First we process the loading phase
    generate_ycsb_plots 5nodes_workload load

    # Then the running phase
    generate_ycsb_plots 5nodes_workload run
  fi

  # Move all the pngs to OUT_DIR.
  mv *.png $OUTDIR/

  # Generate an HTML file aggregating the PNGs.
  # Mostly for local usage, but somewhat useful to check the Jenkins runs too.
  pushd $OUTDIR/
  PNGS=$(ls *.png)
  echo -n > "$OUTDIR/$HTML_FILE"
  echo "<title>Kudu Benchmarks</title>" >> "$OUTDIR/$HTML_FILE"
  echo "<h1 align=center>Kudu Benchmarks</h1>" >> "$OUTDIR/$HTML_FILE"
  for png in $PNGS; do
    echo "<img src=$png><br>" >> "$OUTDIR/$HTML_FILE"
  done
  popd

  popd
  popd
  popd
}

build_run_record() {
  local BUILD_IDENTIFIER=$1
  build_kudu
  run_benchmarks
  parse_and_record_all_results "$BUILD_IDENTIFIER"
}

git_checkout() {
  local GIT_HASH=$1
  git checkout $GIT_HASH
}

run() {

  # Parse command-line options.
  if [ -n "$1" ]; then
    [ "$1" = "-local" ] || usage_and_die
    shift

    BENCHMARK_MODE=$MODE_LOCAL

    # If no hashes are provided, run against the current HEAD.
    if [ -z "$1" ]; then
      build_run_record "working_tree"
    else
      # Convert the passed-in git refs into their hashes.
      # This allows you to use "HEAD~3 HEAD" as arguments
      # and end up with those being evaluated with regard to
      # the _current_ branch, instead of evaluating the second
      # "HEAD" after checking out the first.
      local ref
      local hashes
      for ref in "$@" ; do
        hashes="$hashes $(git rev-parse "$ref")"
      done
      set $hashes
      while [ -n "$1" ]; do
        local GIT_HASH="$1"
        shift
        git_checkout "$GIT_HASH"
        build_run_record "$GIT_HASH"
      done
    fi

  else
    [ -n "$BUILD_NUMBER" ] || usage_and_die
    build_run_record "$BUILD_NUMBER"
  fi

  # The last step is the same for both modes.
  load_stats_and_generate_plots
}

################################################################
# main
################################################################

# Figure out where we are, store in global variables.
LOGDIR="$BASE_DIR/$LOG_DIR_NAME"
OUTDIR="$BASE_DIR/$OUT_DIR_NAME"

# Ensure we are in KUDU_HOME
if [ ! -f "$BASE_DIR/LICENSE.txt" ]; then
  echo "Error: must run from top of Kudu source tree"
  usage_and_die
fi

if ! check_tools; then
  echo "Error: could not find necessary tools to run benchmarks"
  exit 1
fi

# Set up environment.
ulimit -m $[3000*1000]
ulimit -c unlimited   # gather core dumps

# Set CPU governor, and restore it on exit.
old_governor=$(ensure_cpu_scaling performance)
restore_governor() {
  ensure_cpu_scaling $old_governor >/dev/null
}
trap restore_governor EXIT

# Kick off the benchmark script.
run $*

exit 0
