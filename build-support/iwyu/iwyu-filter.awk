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
# This is an awk script to process output from the include-what-you-use (IWYU)
# tool. As of now, IWYU is of alpha quality and it gives many incorrect
# recommendations -- obviously invalid or leading to compilation breakage.
# Most of those can be silenced using appropriate IWYU pragmas, but it's not
# the case for the auto-generated files.
#
# Also, it's possible to address invalid recommendation using mappings:
#   https://github.com/include-what-you-use/include-what-you-use/blob/master/docs/IWYUMappings.md
#
# We are using mappings for the boost library (comes with IWYU) and a few
# custom mappings for gflags, glog, and gtest libraries to address some IWYU
# quirks (hopefully, those should be resolved as IWYU gets better).
#
# Usage:
#  1. Run the CMake with -DCMAKE_CXX_INCLUDE_WHAT_YOU_USE=<iwyu_cmd_line>
#
#     The path to the IWYU binary should be absolute. The path to the binary
#     and the command-line options should be separated by semicolon
#     (that's for feeding it into CMake list variables).
#
#     E.g., from the build directory (line breaks are just for readability):
#
#     CC=../../thirdparty/clang-toolchain/bin/clang
#     CXX=../../thirdparty/clang-toolchain/bin/clang++
#     IWYU="`pwd`../../thirdparty/clang-toolchain/bin/include-what-you-use;\
#       -Xiwyu;--mapping_file=`pwd`../../build-support/iwyu/mappings/map.imp"
#
#     ../../build-support/enable_devtoolset.sh \
#       env CC=$CC CXX=$CXX \
#       ../../thirdparty/installed/common/bin/cmake \
#       -DCMAKE_CXX_INCLUDE_WHAT_YOU_USE=\"$IWYU\" \
#       ../..
#
#     NOTE:
#       Since the Kudu code has some 'ifdef NDEBUG' directives, it's possible
#       that IWYU would produce different results if run against release, not
#       debug build. However, we plan to use the tool only with debug builds.
#
#  2. Run make, separating the output from the IWYU tool into a separate file
#     (it's possible to use piping the output from the tool to the script
#      but having a file is good for future reference, if necessary):
#
#     make -j$(nproc) 2>/tmp/iwyu.log
#
#  3. Process the output from the IWYU tool using the script:
#
#     awk -f ../../build-support/iwyu/iwyu-filter.awk /tmp/iwyu.log
#

BEGIN {
  # This is the list of the files for which the suggestions from IWYU are
  # ignored. Eventually, this list should become empty as soon as all the valid
  # suggestions are addressed and invalid ones are taken care either by proper
  # IWYU pragmas or adding special mappings (e.g. like boost mappings).
  muted["kudu/benchmarks/tpch/rpc_line_item_dao-test.cc"]
  muted["kudu/cfile/block_cache-test.cc"]
  muted["kudu/cfile/bloomfile-test.cc"]
  muted["kudu/cfile/cfile-test.cc"]
  muted["kudu/cfile/cfile_writer.h"]
  muted["kudu/cfile/encoding-test.cc"]
  muted["kudu/cfile/index-test.cc"]
  muted["kudu/cfile/mt-bloomfile-test.cc"]
  muted["kudu/client/client-internal.cc"]
  muted["kudu/client/client-internal.h"]
  muted["kudu/client/client-test.cc"]
  muted["kudu/client/meta_cache.h"]
  muted["kudu/client/predicate-test.cc"]
  muted["kudu/client/scan_token-test.cc"]
  muted["kudu/clock/hybrid_clock-test.cc"]
  muted["kudu/clock/logical_clock-test.cc"]
  muted["kudu/codegen/codegen-test.cc"]
  muted["kudu/common/column_predicate-test.cc"]
  muted["kudu/common/encoded_key-test.cc"]
  muted["kudu/common/generic_iterators-test.cc"]
  muted["kudu/common/key_util-test.cc"]
  muted["kudu/common/partial_row-test.cc"]
  muted["kudu/common/partition-test.cc"]
  muted["kudu/common/partition_pruner-test.cc"]
  muted["kudu/common/row_changelist-test.cc"]
  muted["kudu/common/row_operations-test.cc"]
  muted["kudu/common/scan_spec.cc"]
  muted["kudu/common/scan_spec-test.cc"]
  muted["kudu/common/schema-test.cc"]
  muted["kudu/common/schema.cc"]
  muted["kudu/common/schema.h"]
  muted["kudu/common/types-test.cc"]
  muted["kudu/common/wire_protocol-test.cc"]
  muted["kudu/common/wire_protocol.cc"]
  muted["kudu/common/wire_protocol.h"]
  muted["kudu/consensus/consensus_meta-test.cc"]
  muted["kudu/consensus/consensus_meta_manager-stress-test.cc"]
  muted["kudu/consensus/consensus_meta_manager-test.cc"]
  muted["kudu/consensus/consensus_peers-test.cc"]
  muted["kudu/consensus/consensus_queue-test.cc"]
  muted["kudu/consensus/consensus_queue.cc"]
  muted["kudu/consensus/leader_election-test.cc"]
  muted["kudu/consensus/log-test.cc"]
  muted["kudu/consensus/log_anchor_registry-test.cc"]
  muted["kudu/consensus/log_cache-test.cc"]
  muted["kudu/consensus/log_index-test.cc"]
  muted["kudu/consensus/mt-log-test.cc"]
  muted["kudu/consensus/raft_consensus.h"]
  muted["kudu/consensus/raft_consensus_quorum-test.cc"]
  muted["kudu/consensus/time_manager-test.cc"]
  muted["kudu/experiments/merge-test.cc"]
  muted["kudu/experiments/rwlock-perf.cc"]
  muted["kudu/fs/block_manager-stress-test.cc"]
  muted["kudu/fs/block_manager-test.cc"]
  muted["kudu/fs/block_manager_util-test.cc"]
  muted["kudu/fs/data_dirs-test.cc"]
  muted["kudu/fs/fs_manager-test.cc"]
  muted["kudu/fs/log_block_manager-test.cc"]
  muted["kudu/fs/log_block_manager.cc"]
  muted["kudu/fs/log_block_manager.h"]
  muted["kudu/integration-tests/all_types-itest.cc"]
  muted["kudu/integration-tests/alter_table-randomized-test.cc"]
  muted["kudu/integration-tests/alter_table-test.cc"]
  muted["kudu/integration-tests/authn_token_expire-itest.cc"]
  muted["kudu/integration-tests/catalog_manager_tsk-itest.cc"]
  muted["kudu/integration-tests/client-negotiation-failover-itest.cc"]
  muted["kudu/integration-tests/client-stress-test.cc"]
  muted["kudu/integration-tests/client_failover-itest.cc"]
  muted["kudu/integration-tests/cluster_itest_util.cc"]
  muted["kudu/integration-tests/cluster_verifier.cc"]
  muted["kudu/integration-tests/consistency-itest.cc"]
  muted["kudu/integration-tests/create-table-itest.cc"]
  muted["kudu/integration-tests/create-table-stress-test.cc"]
  muted["kudu/integration-tests/delete_table-itest.cc"]
  muted["kudu/integration-tests/delete_tablet-itest.cc"]
  muted["kudu/integration-tests/dense_node-itest.cc"]
  muted["kudu/integration-tests/disk_reservation-itest.cc"]
  muted["kudu/integration-tests/exactly_once_writes-itest.cc"]
  muted["kudu/integration-tests/external_mini_cluster-itest-base.cc"]
  muted["kudu/integration-tests/external_mini_cluster-itest-base.h"]
  muted["kudu/integration-tests/external_mini_cluster-test.cc"]
  muted["kudu/integration-tests/external_mini_cluster.cc"]
  muted["kudu/integration-tests/external_mini_cluster.h"]
  muted["kudu/integration-tests/external_mini_cluster_fs_inspector.cc"]
  muted["kudu/integration-tests/flex_partitioning-itest.cc"]
  muted["kudu/integration-tests/full_stack-insert-scan-test.cc"]
  muted["kudu/integration-tests/fuzz-itest.cc"]
  muted["kudu/integration-tests/internal_mini_cluster-itest-base.cc"]
  muted["kudu/integration-tests/internal_mini_cluster-itest-base.h"]
  muted["kudu/integration-tests/internal_mini_cluster.h"]
  muted["kudu/integration-tests/linked_list-test.cc"]
  muted["kudu/integration-tests/log-rolling-itest.cc"]
  muted["kudu/integration-tests/log_verifier.cc"]
  muted["kudu/integration-tests/master-stress-test.cc"]
  muted["kudu/integration-tests/master_cert_authority-itest.cc"]
  muted["kudu/integration-tests/master_failover-itest.cc"]
  muted["kudu/integration-tests/master_migration-itest.cc"]
  muted["kudu/integration-tests/master_replication-itest.cc"]
  muted["kudu/integration-tests/minidump_generation-itest.cc"]
  muted["kudu/integration-tests/multidir_cluster-itest.cc"]
  muted["kudu/integration-tests/open-readonly-fs-itest.cc"]
  muted["kudu/integration-tests/raft_consensus-itest.cc"]
  muted["kudu/integration-tests/registration-test.cc"]
  muted["kudu/integration-tests/security-faults-itest.cc"]
  muted["kudu/integration-tests/security-itest.cc"]
  muted["kudu/integration-tests/security-unknown-tsk-itest.cc"]
  muted["kudu/integration-tests/table_locations-itest.cc"]
  muted["kudu/integration-tests/tablet_copy-itest.cc"]
  muted["kudu/integration-tests/tablet_copy_client_session-itest.cc"]
  muted["kudu/integration-tests/tablet_history_gc-itest.cc"]
  muted["kudu/integration-tests/tablet_replacement-itest.cc"]
  muted["kudu/integration-tests/test_workload.cc"]
  muted["kudu/integration-tests/test_workload.h"]
  muted["kudu/integration-tests/token_signer-itest.cc"]
  muted["kudu/integration-tests/ts_recovery-itest.cc"]
  muted["kudu/integration-tests/ts_tablet_manager-itest.cc"]
  muted["kudu/integration-tests/update_scan_delta_compact-test.cc"]
  muted["kudu/integration-tests/version_migration-test.cc"]
  muted["kudu/integration-tests/webserver-stress-itest.cc"]
  muted["kudu/integration-tests/write_throttling-itest.cc"]
  muted["kudu/master/catalog_manager-test.cc"]
  muted["kudu/master/catalog_manager.cc"]
  muted["kudu/master/master-test.cc"]
  muted["kudu/master/mini_master-test.cc"]
  muted["kudu/master/sys_catalog-test.cc"]
  muted["kudu/rpc/client_negotiation.cc"]
  muted["kudu/rpc/messenger.h"]
  muted["kudu/rpc/negotiation-test.cc"]
  muted["kudu/rpc/negotiation.cc"]
  muted["kudu/rpc/protoc-gen-krpc.cc"]
  muted["kudu/rpc/reactor.cc"]
  muted["kudu/rpc/reactor.h"]
  muted["kudu/rpc/remote_user.cc"]
  muted["kudu/rpc/request_tracker-test.cc"]
  muted["kudu/rpc/result_tracker.h"]
  muted["kudu/rpc/rpc-test.cc"]
  muted["kudu/rpc/rpc_sidecar.cc"]
  muted["kudu/rpc/rpc_sidecar.h"]
  muted["kudu/rpc/rpc_stub-test.cc"]
  muted["kudu/rpc/server_negotiation.cc"]
  muted["kudu/rpc/service_if.h"]
  muted["kudu/rpc/service_pool.cc"]
  muted["kudu/rpc/service_queue-test.cc"]
  muted["kudu/rpc/service_queue.h"]
  muted["kudu/security/ca/cert_management-test.cc"]
  muted["kudu/security/ca/cert_management.cc"]
  muted["kudu/security/ca/cert_management.h"]
  muted["kudu/security/cert-test.cc"]
  muted["kudu/security/cert.cc"]
  muted["kudu/security/crypto-test.cc"]
  muted["kudu/security/openssl_util.h"]
  muted["kudu/security/test/mini_kdc-test.cc"]
  muted["kudu/security/tls_context.cc"]
  muted["kudu/security/tls_context.h"]
  muted["kudu/security/tls_handshake-test.cc"]
  muted["kudu/security/tls_handshake.cc"]
  muted["kudu/security/token-test.cc"]
  muted["kudu/server/default-path-handlers.cc"]
  muted["kudu/server/rpc_server-test.cc"]
  muted["kudu/server/server_base.cc"]
  muted["kudu/server/webserver-test.cc"]
  muted["kudu/server/webserver.cc"]
  muted["kudu/tablet/all_types-scan-correctness-test.cc"]
  muted["kudu/tablet/cbtree-test.cc"]
  muted["kudu/tablet/cfile_set-test.cc"]
  muted["kudu/tablet/cfile_set.cc"]
  muted["kudu/tablet/compaction-test.cc"]
  muted["kudu/tablet/compaction_policy-test.cc"]
  muted["kudu/tablet/composite-pushdown-test.cc"]
  muted["kudu/tablet/delta_compaction-test.cc"]
  muted["kudu/tablet/deltafile-test.cc"]
  muted["kudu/tablet/deltamemstore-test.cc"]
  muted["kudu/tablet/diskrowset-test.cc"]
  muted["kudu/tablet/diskrowset.cc"]
  muted["kudu/tablet/lock_manager-test.cc"]
  muted["kudu/tablet/major_delta_compaction-test.cc"]
  muted["kudu/tablet/memrowset-test.cc"]
  muted["kudu/tablet/memrowset.h"]
  muted["kudu/tablet/mt-diskrowset-test.cc"]
  muted["kudu/tablet/mt-rowset_delta_compaction-test.cc"]
  muted["kudu/tablet/mt-tablet-test.cc"]
  muted["kudu/tablet/mvcc-test.cc"]
  muted["kudu/tablet/rowset_tree-test.cc"]
  muted["kudu/tablet/tablet-decoder-eval-test.cc"]
  muted["kudu/tablet/tablet-pushdown-test.cc"]
  muted["kudu/tablet/tablet-schema-test.cc"]
  muted["kudu/tablet/tablet-test.cc"]
  muted["kudu/tablet/tablet.cc"]
  muted["kudu/tablet/tablet.h"]
  muted["kudu/tablet/tablet_bootstrap-test.cc"]
  muted["kudu/tablet/tablet_history_gc-test.cc"]
  muted["kudu/tablet/tablet_metadata-test.cc"]
  muted["kudu/tablet/tablet_mm_ops-test.cc"]
  muted["kudu/tablet/tablet_random_access-test.cc"]
  muted["kudu/tablet/tablet_replica-test.cc"]
  muted["kudu/tablet/tablet_throttle-test.cc"]
  muted["kudu/tablet/transactions/transaction_tracker-test.cc"]
  muted["kudu/tools/ksck-test.cc"]
  muted["kudu/tools/ksck.cc"]
  muted["kudu/tools/ksck.h"]
  muted["kudu/tools/ksck_remote-test.cc"]
  muted["kudu/tools/ksck_remote.cc"]
  muted["kudu/tools/kudu-admin-test.cc"]
  muted["kudu/tools/kudu-tool-test.cc"]
  muted["kudu/tools/kudu-ts-cli-test.cc"]
  muted["kudu/tools/tool_action-test.cc"]
  muted["kudu/tools/tool_action.cc"]
  muted["kudu/tools/tool_action_local_replica.cc"]
  muted["kudu/tools/tool_action_master.cc"]
  muted["kudu/tools/tool_action_pbc.cc"]
  muted["kudu/tools/tool_action_perf.cc"]
  muted["kudu/tools/tool_action_remote_replica.cc"]
  muted["kudu/tools/tool_action_table.cc"]
  muted["kudu/tools/tool_action_tablet.cc"]
  muted["kudu/tools/tool_action_tserver.cc"]
  muted["kudu/tools/tool_action_wal.cc"]
  muted["kudu/tools/tool_main.cc"]
  muted["kudu/tserver/heartbeater.cc"]
  muted["kudu/tserver/mini_tablet_server-test.cc"]
  muted["kudu/tserver/scanners-test.cc"]
  muted["kudu/tserver/tablet_copy_client-test.cc"]
  muted["kudu/tserver/tablet_copy_service-test.cc"]
  muted["kudu/tserver/tablet_copy_source_session-test.cc"]
  muted["kudu/tserver/tablet_server-stress-test.cc"]
  muted["kudu/tserver/tablet_server-test.cc"]
  muted["kudu/tserver/tablet_service.cc"]
  muted["kudu/tserver/ts_tablet_manager-test.cc"]
  muted["kudu/twitter-demo/ingest_firehose.cc"]
  muted["kudu/twitter-demo/parser-test.cc"]
  muted["kudu/util/atomic-test.cc"]
  muted["kudu/util/bit-util-test.cc"]
  muted["kudu/util/bitmap-test.cc"]
  muted["kudu/util/blocking_queue-test.cc"]
  muted["kudu/util/bloom_filter-test.cc"]
  muted["kudu/util/cache-test.cc"]
  muted["kudu/util/compression/compression-test.cc"]
  muted["kudu/util/crc-test.cc"]
  muted["kudu/util/easy_json-test.cc"]
  muted["kudu/util/env-test.cc"]
  muted["kudu/util/env_util-test.cc"]
  muted["kudu/util/failure_detector-test.cc"]
  muted["kudu/util/faststring-test.cc"]
  muted["kudu/util/file_cache-stress-test.cc"]
  muted["kudu/util/file_cache-test.cc"]
  muted["kudu/util/flag_tags-test.cc"]
  muted["kudu/util/flag_validators-test.cc"]
  muted["kudu/util/flags-test.cc"]
  muted["kudu/util/group_varint-test.cc"]
  muted["kudu/util/inline_slice-test.cc"]
  muted["kudu/util/interval_tree-test.cc"]
  muted["kudu/util/jsonreader-test.cc"]
  muted["kudu/util/jsonwriter-test.cc"]
  muted["kudu/util/knapsack_solver-test.cc"]
  muted["kudu/util/logging-test.cc"]
  muted["kudu/util/maintenance_manager-test.cc"]
  muted["kudu/util/mem_tracker-test.cc"]
  muted["kudu/util/memory/arena-test.cc"]
  muted["kudu/util/metrics-test.cc"]
  muted["kudu/util/minidump-test.cc"]
  muted["kudu/util/minidump.cc"]
  muted["kudu/util/mt-hdr_histogram-test.cc"]
  muted["kudu/util/mt-metrics-test.cc"]
  muted["kudu/util/mt-threadlocal-test.cc"]
  muted["kudu/util/net/net_util-test.cc"]
  muted["kudu/util/object_pool-test.cc"]
  muted["kudu/util/once-test.cc"]
  muted["kudu/util/os-util-test.cc"]
  muted["kudu/util/pb_util-test.cc"]
  muted["kudu/util/process_memory-test.cc"]
  muted["kudu/util/process_memory.cc"]
  muted["kudu/util/pstack_watcher-test.cc"]
  muted["kudu/util/resettable_heartbeater-test.cc"]
  muted["kudu/util/rle-test.cc"]
  muted["kudu/util/rolling_log-test.cc"]
  muted["kudu/util/spinlock_profiling-test.cc"]
  muted["kudu/util/stack_watchdog-test.cc"]
  muted["kudu/util/subprocess-test.cc"]
  muted["kudu/util/thread-test.cc"]
  muted["kudu/util/threadpool-test.cc"]
  muted["kudu/util/throttler-test.cc"]
  muted["kudu/util/trace-test.cc"]
}

# mute all suggestions for the auto-generated files
/.*\.(pb|proxy|service)\.(cc|h) should (add|remove) these lines:/, /^$/ {
  next
}

# mute suggestions for the explicitly specified files
/.* should (add|remove) these lines:/ {
  do_print = 1
  for (path in muted) {
    if (index($0, path)) {
      do_print = 0
      break
    }
  }
}
/^$/ {
  if (do_print) print
  do_print = 0
}
{ if (do_print) print }
