# Copyright (c) 2012, Cloudera, inc.

set(TABLET_SRCS
  tablet.cc
  tablet_bootstrap.cc
  tablet_metrics.cc
  tablet_peer.cc
  transactions/transaction.cc
  transactions/alter_schema_transaction.cc
  transactions/change_config_transaction.cc
  transactions/transaction_driver.cc
  transactions/transaction_tracker.cc
  transactions/write_transaction.cc
  cfile_set.cc
  compaction.cc
  compaction_policy.cc
  delta_key.cc
  diskrowset.cc
  lock_manager.cc
  maintenance_manager.cc
  memrowset.cc
  multi_column_writer.cc
  mutation.cc
  mvcc.cc
  row_op.cc
  rowset.cc
  rowset_info.cc
  rowset_tree.cc
  svg_dump.cc
  tablet_metadata.cc
  rowset_metadata.cc
  deltafile.cc
  deltamemstore.cc
  delta_applier.cc
  delta_compaction.cc
  delta_iterator_merger.cc
  delta_stats.cc
  delta_store.cc
  delta_tracker.cc
)

PROTOBUF_GENERATE_CPP(TABLET_PROTO_SRCS TABLET_PROTO_HDRS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES
    tablet.proto
    metadata.proto)

add_library(tablet_proto ${TABLET_PROTO_SRCS} ${TABLET_PROTO_HDRS})
target_link_libraries(tablet_proto
  fs_proto
  metadata_proto
  kudu_common)

set(TABLET_SRCS ${TABLET_SRCS} ${TABLET_PROTO_SRCS})
add_library(tablet ${TABLET_SRCS})
target_link_libraries(tablet
  codegen
  kudu_common
  cfile
  gutil
  server_common
  kudu_fs
  kudu_util
  consensus)

set(KUDU_TEST_LINK_LIBS tablet ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(tablet-test)
ADD_KUDU_TEST(mt-tablet-test RUN_SERIAL true)
ADD_KUDU_TEST(compaction_policy-test)
ADD_KUDU_TEST(diskrowset-test)
ADD_KUDU_TEST(mt-diskrowset-test RUN_SERIAL true)
ADD_KUDU_TEST(memrowset-test)
ADD_KUDU_TEST(deltamemstore-test)
ADD_KUDU_TEST(deltafile-test)
ADD_KUDU_TEST(cfile_set-test)
ADD_KUDU_TEST(tablet-pushdown-test)
ADD_KUDU_TEST(tablet-schema-test)
ADD_KUDU_TEST(tablet_bootstrap-test)
ADD_KUDU_TEST(maintenance_manager-test)
ADD_KUDU_TEST(metadata-test)
ADD_KUDU_TEST(mvcc-test)
ADD_KUDU_TEST(compaction-test)
ADD_KUDU_TEST(lock_manager-test)
ADD_KUDU_TEST(rowset_tree-test)
ADD_KUDU_TEST(composite-pushdown-test)
ADD_KUDU_TEST(delta_compaction-test)
ADD_KUDU_TEST(mt-rowset_delta_compaction-test)
ADD_KUDU_TEST(major_delta_compaction-test)
ADD_KUDU_TEST(transactions/transaction_tracker-test)
ADD_KUDU_TEST(tablet_peer-test)

# Some tests don't have dependencies on other tablet stuff
set(KUDU_TEST_LINK_LIBS kudu_util gutil ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(cbtree-test RUN_SERIAL true)
