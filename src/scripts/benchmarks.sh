#!/bin/bash -xe
########################################################################
# Copyright (c) 2014 Cloudera, Inc.
#
# Run and compare benchmarks.
#
# Allows for running comparisons either locally or as part of a
# Jenkins job which integrates with a historical stats DB.
# Run this script with -help for usage information.
#
# Jenkins job: http://sandbox.jenkins.cloudera.com/job/kudu-benchmarks
########################################################################

################################################################
# Constants
################################################################

MODE_JENKINS="jenkins"
MODE_LOCAL="local"

LOCAL_STATS_BASE="local-stats"

MT_TABLET_TEST=mt-tablet-test
RPC_BENCH_TEST=RpcBenchBenchmark
CBTREE_TEST=cbtree-test
BLOOM_TEST=BloomfileBenchmark
WIRE_PROTOCOL_TEST=WireProtocolBenchmark
COMPACT_MERGE_BENCH=CompactBenchMerge
WITH_OVERLAP=Overlap
NO_OVERLAP=NoOverlap

LOG_DIR_NAME=build/bench-logs
OUT_DIR_NAME=build/bench-out
HTML_FILE="benchmarks.html"

################################################################
# Global variables
################################################################

BENCHMARK_MODE=$MODE_JENKINS # we default to "jenkins mode"
BASE_DIR=""
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
    # Get last month of stats
    python get-job-stats-from-mysql.py $TEST_NAME 1
  else
    # Convert MySQL wildcards to shell wildcards.
    local TEST_NAME=$(echo $TEST_NAME | perl -pe 's/%/*/g')
    local STATS_FILES=$(ls $OUTDIR/$LOCAL_STATS_BASE-$TEST_NAME.tsv)
    # Note: literal tabs in below string.
    echo "workload	avg_runtime	build_number"
    for f in $STATS_FILES; do
      cat $f
    done
  fi
}

write_img_plot() {
  local INPUT_FILE=$1
  local TEST_NAME=$2
  Rscript jobs_runtime.R $INPUT_FILE $TEST_NAME
}

write_mttablet_img_plots() {
  local INPUT_FILE=$1
  local TEST_NAME=$2
  xvfb-run Rscript mt-tablet-test-graph.R $INPUT_FILE $TEST_NAME
}

build_kudu() {
  # Build thirdparty
  pushd thirdparty
  ./build-if-necessary.sh
  popd

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
}

run_benchmarks() {

  # run mt-tablet-tests 0 through 9
  ./build/latest/mt-tablet-test --gtest_filter=\*DoTestAllAtOnce\* --num_counter_threads=0 \
    --flush_threshold_mb=32 --memrowset_throttle_mb=256 --num_slowreader_threads=0 \
    --flusher_backoff=1.0 --flusher_initial_frequency_ms=1000 --inserts_per_thread=1000000 \
    &> $LOGDIR/${MT_TABLET_TEST}.log

  # run rpc-bench test 5 times. 10 seconds per run
  for i in {0..4}; do
    KUDU_ALLOW_SLOW_TESTS=true ./build/latest/rpc-bench &> $LOGDIR/$RPC_BENCH_TEST$i.log
  done

  # run cbtree-test 5 times. 20 seconds per run
  for i in {0..4}; do
    KUDU_ALLOW_SLOW_TESTS=true ./build/latest/cbtree-test \
      --gtest_filter=TestCBTree.TestScanPerformance &> $LOGDIR/${CBTREE_TEST}$i.log
  done

  # run bloomfile-test 5 times. ~3.3 seconds per run
  for i in {0..4}; do
    ./build/latest/bloomfile-test --benchmark_queries=10000000 --bloom_size_bytes=32768 \
      --n_keys=100000 --gtest_filter=*Benchmark &> $LOGDIR/$BLOOM_TEST$i.log
  done

  # run wire_protocol-test 5 times. 6 seconds per run
  for i in {0..4}; do
    KUDU_ALLOW_SLOW_TESTS=true ./build/latest/wire_protocol-test --gtest_filter=*Benchmark \
      &> $LOGDIR/$WIRE_PROTOCOL_TEST$i.log
  done

  # run compaction-test 5 times, 6 seconds each
  for i in {0..4}; do
    KUDU_ALLOW_SLOW_TESTS=true ./build/latest/compaction-test \
      --gtest_filter=TestCompaction.BenchmarkMerge* &> $LOGDIR/${COMPACT_MERGE_BENCH}$i.log
  done
}

parse_and_record_all_results() {
  local BUILD_IDENTIFIER="$1"

  if [ -z "$BUILD_IDENTIFIER" ]; then
    echo "ERROR: BUILD_IDENTIFIER not defined"
    exit 1
  fi

  pushd src
  pushd scripts

  # parse the number of ms out of "[       OK ] MultiThreadedTabletTest/9.DoTestAllAtOnce (14966 ms)"
  local MT_TABLET_TEST_TIMINGS="${MT_TABLET_TEST}-timings"
  grep OK $LOGDIR/${MT_TABLET_TEST}.log | cut -d "(" -f2 | cut -d ")" -f1 | cut -d " " -f1 \
    > $LOGDIR/${MT_TABLET_TEST_TIMINGS}.txt

  # The tests go from 0 to 9, but files start at line one so we add +1 to the line number. Then using the
  # timing we found, we multiply it by 1000 to gets seconds in float, then send it to MySQL
  for i in {0..9}; do
    linenumber=$[ $i + 1 ]
    timing=`sed -n "${linenumber}p" $LOGDIR/${MT_TABLET_TEST_TIMINGS}.txt`
    record_result $BUILD_IDENTIFIER MultiThreadedTabletTest_$i 1 `echo $timing / 1000 | bc -l`
  done

  # parse out the real time from: "Times for Insert 10000000 keys: real 16.438s user 16.164s  sys 0.229s"
  for i in {0..4}; do
    real=`grep "Times for Insert" $LOGDIR/${CBTREE_TEST}$i.log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ConcurrentBTreeScanInsert $i $real
  done

  for i in {0..4}; do
    real=`grep "not frozen" $LOGDIR/${CBTREE_TEST}$i.log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ConcurrentBTreeScanNotFrozen $i $real
  done

  for i in {0..4}; do
    real=`grep "(frozen" $LOGDIR/${CBTREE_TEST}$i.log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ConcurrentBTreeScanFrozen $i $real
  done

  # parse out the real time from "Times for with overlap: real 0.557s user 0.546s sys 0.010s"
  for i in {0..4}; do
    real=`grep "with overlap" $LOGDIR/${COMPACT_MERGE_BENCH}$i.log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ${COMPACT_MERGE_BENCH}${WITH_OVERLAP} $i $real
  done

  for i in {0..4}; do
    real=`grep "without overlap" $LOGDIR/${COMPACT_MERGE_BENCH}$i.log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER ${COMPACT_MERGE_BENCH}${NO_OVERLAP} $i $real
  done

  # Parse out the real time from: "Times for Running 10000000 queries: real 3.281s  user 3.273s sys 0.000s"
  for i in {0..4}; do
    real=`grep "Times for Running" $LOGDIR/$BLOOM_TEST$i.log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER $BLOOM_TEST $i $real
  done

  # Parse out the real time from: "Times for Converting to PB: real 5.962s  user 5.918s sys 0.025s"
  for i in {0..4}; do
    real=`grep "Times for Converting" $LOGDIR/$WIRE_PROTOCOL_TEST$i.log | ./parse_real_out.sh`
    record_result $BUILD_IDENTIFIER $WIRE_PROTOCOL_TEST $i $real
  done

  # parse the rate out of: "I1009 15:00:30.023576 27043 rpc-bench.cc:108] Reqs/sec:         84404.4"
  for i in {0..4}; do
    rate=`grep Reqs $LOGDIR/$RPC_BENCH_TEST$i.log | cut -d ":" -f 5 | tr -d ' '`
    record_result $BUILD_IDENTIFIER $RPC_BENCH_TEST $i $rate
  done

  popd
  popd
}

load_stats_and_generate_plots() {
  pushd src
  pushd scripts

  load_stats "%MultiThreadedTabletTest%" > $OUTDIR/mt-tablet-test-runtime.tsv
  write_img_plot $OUTDIR/mt-tablet-test-runtime.tsv ${MT_TABLET_TEST}

  load_stats ConcurrentBTreeScanInsert > $OUTDIR/cb-tree-insert.tsv
  write_img_plot $OUTDIR/cb-tree-insert.tsv cb-tree-insert

  load_stats ConcurrentBTreeScanNotFrozen > $OUTDIR/cb-ctree-not-frozen.tsv
  write_img_plot $OUTDIR/cb-ctree-not-frozen.tsv cb-ctree-not-frozen

  load_stats ConcurrentBTreeScanFrozen > $OUTDIR/cb-ctree-frozen.tsv
  write_img_plot $OUTDIR/cb-ctree-frozen.tsv cb-ctree-frozen

  load_stats ${COMPACT_MERGE_BENCH}${WITH_OVERLAP} > $OUTDIR/${COMPACT_MERGE_BENCH}${WITH_OVERLAP}.tsv
  write_img_plot $OUTDIR/${COMPACT_MERGE_BENCH}${WITH_OVERLAP}.tsv ${COMPACT_MERGE_BENCH}${WITH_OVERLAP}

  load_stats ${COMPACT_MERGE_BENCH}${NO_OVERLAP} > $OUTDIR/${COMPACT_MERGE_BENCH}${NO_OVERLAP}.tsv
  write_img_plot $OUTDIR/${COMPACT_MERGE_BENCH}${NO_OVERLAP}.tsv ${COMPACT_MERGE_BENCH}${NO_OVERLAP}

  load_stats $BLOOM_TEST > $OUTDIR/$BLOOM_TEST.tsv
  write_img_plot $OUTDIR/$BLOOM_TEST.tsv $BLOOM_TEST

  load_stats $WIRE_PROTOCOL_TEST > $OUTDIR/$WIRE_PROTOCOL_TEST.tsv
  write_img_plot $OUTDIR/$WIRE_PROTOCOL_TEST.tsv $WIRE_PROTOCOL_TEST

  load_stats $RPC_BENCH_TEST > $OUTDIR/$RPC_BENCH_TEST.tsv
  write_img_plot $OUTDIR/$RPC_BENCH_TEST.tsv $RPC_BENCH_TEST

  # Generate all the pngs for all the mt-tablet tests
  for i in {0..9}; do
    cat $LOGDIR/${MT_TABLET_TEST}.log | ./graph-metrics.py MultiThreadedTabletTest/$i > $OUTDIR/test$i.tsv
    # Don't bail on failure (why not?)
    write_mttablet_img_plots $OUTDIR/test$i.tsv test$i || true
  done

  if [ "${BENCHMARK_MODE}" = "${MODE_JENKINS}" ]; then
    ################################################################
    # Plot the separately-recorded TPCH graphs as well
    # (only for Jenkins)
    ################################################################

    # TPC-H 1 runs separately, let's just get those graphs
    load_stats query_1_1gb > $OUTDIR/tpch1-query.tsv
    write_img_plot $OUTDIR/tpch1-query.tsv tpch1-query

    load_stats insert_1gb > $OUTDIR/tpch1-insert.tsv
    write_img_plot $OUTDIR/tpch1-insert.tsv tpch1-insert
  fi

  # Move all the pngs to OUT_DIR.
  mv *.png $OUTDIR/

  # Generate an HTML file aggregating the PNGs in local mode.
  if [ "${BENCHMARK_MODE}" = "${MODE_LOCAL}" ]; then
    pushd $OUTDIR/
    PNGS=$(ls *.png)
    echo -n > "$OUTDIR/$HTML_FILE"
    echo "<title>Kudu Benchmarks</title>" >> "$OUTDIR/$HTML_FILE"
    echo "<h1 align=center>Kudu Benchmarks</h1>" >> "$OUTDIR/$HTML_FILE"
    for png in $PNGS; do
      echo "<img src=$png><br>" >> "$OUTDIR/$HTML_FILE"
    done
    popd
  fi

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

    # Must specify at least one git hash
    [ -n "$1" ] || usage_and_die

    while [ -n "$1" ]; do
      local GIT_HASH="$1"
      shift
      git_checkout "$GIT_HASH"
      build_run_record "$GIT_HASH"
    done

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
BASE_DIR=$(pwd)
LOGDIR="$BASE_DIR/$LOG_DIR_NAME"
OUTDIR="$BASE_DIR/$OUT_DIR_NAME"

# Ensure we are in KUDU_HOME
if [ ! -f "$BASE_DIR/LICENSE.txt" ]; then
  set +x
  echo "Error: must run from top of Kudu source tree"
  usage_and_die
fi

# Set up environment.
ulimit -m $[3000*1000]
ulimit -c unlimited   # gather core dumps

export PATH=$BASE_DIR/thirdparty/installed/bin:$PATH
export PPROF_PATH=$BASE_DIR/thirdparty/installed/bin/pprof

# Create output directories if needed.
[ -d "$LOGDIR" ] || mkdir "$LOGDIR"
[ -d "$OUTDIR" ] || mkdir "$OUTDIR"

# Clean up files from previous runs.
rm -f $LOGDIR/*.log
rm -f $LOGDIR/*.txt
rm -f $OUTDIR/*.tsv
rm -f $OUTDIR/*.png

# Kick off the benchmark script.
run $*

exit 0
