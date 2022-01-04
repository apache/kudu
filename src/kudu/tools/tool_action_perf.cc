// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//
// This is a small load generation tool which generates and pushes data
// to a Kudu cluster as fast as possible. In each run, the tool inserts data
// into a single table. The table might exist already, and the tool then
// populates it with generated data in accordance with the table schema.
// The tool can also create a dedicated temporary table with pre-defined
// schema. By default, dedicated temporary tables are dropped upon successful
// completion of the tool, but they can be optionally preserved. Also,
// a post-insertion scan may be run on the table to verify that the count
// of the inserted rows matches the expected number.
//
// See below for examples of usage.
//
// Run in MANUAL_FLUSH mode, 1 thread inserting 8M rows into auto-created table,
// flushing every 2000 rows, unlimited number of buffers with 32MB
// limit on their total size, with auto-generated strings
// of length 64 for binary and string fields
// with Kudu master server listening on the default port at localhost:
//
//   kudu perf loadgen 127.0.0.1 \
//     --num_threads=1 \
//     --num_rows_per_thread=8000000 \
//     --string_len=64 \
//     --buffer_size_bytes=33554432 \
//     --buffers_num=0 \
//     --flush_per_n_rows=2000 \
//
//
// Run in AUTO_FLUSH_BACKGROUND mode, 2 threads inserting 4M rows each inserting
// into auto-created table, with limit of 8 buffers max 1MB in size total,
// having 12.5% for buffer flush watermark,
// using the specified pre-set string for binary and string fields
// with Kudu master server listening on the default port at localhost:
//
//   kudu perf loadgen 127.0.0.1 \
//     --num_threads=2 \
//     --num_rows_per_thread=4000000 \
//     --string_fixed=012345678901234567890123456789012 \
//     --buffer_size_bytes=1048576 \
//     --buffer_flush_watermark_pct=0.125 \
//     --buffers_num=8
//
//
// Run in AUTO_FLUSH_BACKGROUND mode, 4 threads inserting 2M rows each inserting
// into auto-created table, with limit of 4 buffers max 64KB in size total,
// having 25% for buffer flush watermark,
// using the specified pre-set string for binary and string fields
// with Kudu master server listening at default master RPC port at 127.0.0.1:
//
//   kudu perf loadgen 127.0.0.1 \
//     --num_threads=4 \
//     --num_rows_per_thread=2000000 \
//     --string_fixed=0123456789 \
//     --buffer_size_bytes=65536 \
//     --buffers_num=4 \
//     --buffer_flush_watermark_pct=0.25 \
//     --table_name=bench_02
//
//
// Run with default parameter values for data generation and batching,
// inserting data into auto-created table,
// with Kudu master server listening on the default port at localhost,
// plus run post-insertion row scan to verify
// that the count of the inserted rows matches the expected number:
//
//   kudu perf loadgen 127.0.0.1 \
//     --run_scan=true
//
//
// If running the tool against already existing table multiple times,
// use the '--seq_start' flag to avoid errors on duplicate values in subsequent
// runs. For example: an already existing table 't3' has 5 columns.
// The sequence below contains 3 runs which insert 6000 rows in total
// (3 runs * 1000 rows per thread * 2 threads)
// with no duplicate values across all columns:
//
//   kudu perf loadgen 127.0.0.1 --table_name=t3 --num_threads=2 \
//     --num_rows_per_thread=1000 --seq_start=0
//
//   kudu perf loadgen 127.0.0.1 --table_name=t3 --num_threads=2 \
//     --num_rows_per_thread=1000 --seq_start=10000
//
//   perf perf loadgen 127.0.0.1 --table_name=t3 --num_threads=2 \
//     --num_rows_per_thread=1000 --seq_start=20000
//
// The single sequence number is used to generate values for all table columns,
// so for the example above each run increments the sequence number by 10000:
// 1000 rows per thread * 2 threads * 5 columns
//
// Auto-generated tables can be configured to use hash partitioning, range
// partitioning, or both. Below are a few examples of this.
//
//   kudu perf loadgen 127.0.0.1 --num_threads=8 --num_rows_per_thread=1000 \
//     --table_num_hash_partitions=1 --table_num_range_partitions=8 \
//     --use_random=false
//
// In the above example, a table with eight range partitions will be created,
// each partition will be in charge of 1000 rows worth of values; for a
// three-column table, this means a primary key range width of 3000. Eight
// inserter threads will be created and will insert rows non-randomly; by
// design of the range partitioning splits, this means each thread will insert
// into a single range partition.
//
//   kudu perf loadgen 127.0.0.1 --num_threads=8 --num_rows_per_thread=1000 \
//     --table_num_hash_partitions=8 --table_num_range_partitions=1 \
//     --use_random=false
//
// In the above example, a table with 8 hash partitions will be created.
//
//   kudu perf loadgen 127.0.0.1 --num_threads=8 --num_rows_per_thread=1000 \
//     --table_num_hash_partitions=8 --table_num_range_partitions=8 \
//     --use_random=false
//
// In the above example, a table with a total of 64 tablets will be created.
// The range partitioning splits will be the same as those in the
// range-partitioning-only example.
//
// Below are illustrations of range partitioning and non-random write
// workloads. The y-axis for both the threads and the tablets is the keyspace,
// increasing going downwards.
//
//   --num_threads=2 --table_num_range_partitions=2 --table_num_hash_partitions=1
//
//   Threads sequentially
//   insert to their keyspaces
//   in non-random insert mode.
//      +  +---------+         ^
//      |  | thread1 | tabletA |  Tablets' range partitions are
//      |  |         |         |  set to match the desired total
//      v  +---------+---------+  number of inserted rows for the
//      |  | thread2 | tabletB |  entire workload, but leaving the
//      |  |         |         |  outermost tablets unbounded.
//      v  +---------+         v
//
// If the number of tablets is not a multiple of the number of threads when
// using an auto-generated range-partitioned table, we lose the guarantee
// that we always write to a monotonically increasing range on each tablet.
//
//   --num_threads=2 --table_num_range_partitions=3 --table_num_hash_partitions=1
//
//      +  +---------+         ^
//      |  | thread1 | tabletA |
//      |  |         +---------+
//      v  +---------| tabletB |
//      |  | thread2 +---------+
//      |  |         | tabletC |
//      v  +---------+         v

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/clock/logical_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/types.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tools/table_scanner.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/int128.h"
#include "kudu/util/logging.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

using kudu::ColumnSchema;
using kudu::KuduPartialRow;
using kudu::Stopwatch;
using kudu::TypeInfo;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduTransaction;
using kudu::client::KuduWriteOperation;
using kudu::client::sp::shared_ptr;
using kudu::clock::LogicalClock;
using kudu::consensus::ConsensusBootstrapInfo;
using kudu::consensus::ConsensusMetadata;
using kudu::consensus::ConsensusMetadataManager;
using kudu::log::Log;
using kudu::log::LogAnchorRegistry;
using kudu::tablet::RowIteratorOptions;
using kudu::tablet::Tablet;
using kudu::tablet::TabletMetadata;
using std::cerr;
using std::cout;
using std::endl;
using std::lock_guard;
using std::mutex;
using std::numeric_limits;
using std::ostringstream;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;
using strings::SubstituteAndAppend;

DEFINE_double(buffer_flush_watermark_pct, 0.5,
              "Mutation buffer flush watermark, in percentage of total size.");
DEFINE_int32(buffer_size_bytes, 4 * 1024 * 1024,
             "Size of the mutation buffer, per session (bytes).");
DEFINE_int32(buffers_num, 2,
             "Number of mutation buffers per session.");
DEFINE_int32(error_buffer_size_bytes, 16 * 1024 * 1024,
             "Size of the error buffer, per session (bytes). 0 means 'unlimited'. "
             "This setting may impose an additional upper limit for the "
             "effective number of errors controlled by the "
             "'--show_first_n_errors' flag.");
DEFINE_int32(flush_per_n_rows, 0,
             "Perform async flush per given number of rows added. "
             "Setting to non-zero implicitly turns on manual flush mode.");
DEFINE_bool(keep_auto_table, false,
            "If using the auto-generated table, enabling this option "
            "retains the table populated with the data after the test "
            "finishes. By default, the auto-generated table is dropped "
            "after successfully finishing the test. NOTE: this parameter "
            "has no effect if using already existing table "
            "(see the '--table_name' flag): neither the existing table "
            "nor its data is ever dropped/deleted.");
DEFINE_int32(num_iters, 1,
             "Number of times to run the scan.");
DEFINE_int64(num_rows_per_thread, 1000,
             "Number of rows each thread generates and inserts; "
             "-1 means unlimited. All rows generated by a thread are inserted "
             "in the context of the same session.");
DEFINE_bool(use_client_per_thread,
            true,
            "Use a separate KuduClient instance for each load-generating thread. "
            "This increases throughput by reducing contention on various Client "
            "internals.");
DEFINE_bool(ordered_scan, false,
            "Whether to run an ordered or unordered scan.");
DEFINE_bool(run_scan, false,
            "Whether to run post-insertion scan to verify that the count of "
            "the inserted rows matches the expected number. If enabled, "
            "the scan is run only if no errors were encountered "
            "while inserting the generated rows.");
DEFINE_bool(run_cleanup, false,
            "Whether to run post-insertion deletion to reset the existing "
            "table as before.");
DEFINE_uint64(seq_start, 0,
              "Initial value for the generator in sequential mode. "
              "This is useful when running multiple times against already "
              "existing table: for every next run, set this flag to "
              "(num_threads * num_rows_per_thread * column_num + seq_start).");
DEFINE_int32(show_first_n_errors, 0,
             "Output detailed information on the specified number of "
             "first n errors (if any). The limit on the per-session error "
             "buffer space may impose an additional upper limit for the "
             "effective number of errors in the output. If so, consider "
             "increasing the size of the error buffer using the "
             "'--error_buffer_size_bytes' flag.");
DEFINE_string(string_fixed, "",
              "Pre-defined string to write into binary and string columns. "
              "Client generates more data per second using pre-defined string "
              "compared with auto-generated strings of the same length "
              "if run with the same CPU/memory configuration. If left empty, "
              "then auto-generated strings of length specified by the "
              "'--string_len' parameter are used instead.");
DEFINE_int32(string_len, 32,
             "Length of strings to put into string and binary columns. This "
             "parameter is not in effect if '--string_fixed' is specified.");
DEFINE_string(auto_database, "default",
              "The database in which to create the automatically generated table. "
              "If --table_name is set, this flag has no effect, since a table is "
              "not created. This flag is useful primarily when the Hive Metastore "
              "integration is enabled in the cluster. If empty, no database is "
              "used.");
DEFINE_int32(table_num_hash_partitions, 8,
             "The number of hash partitions to create when this tool creates "
             "a new table. Note: The total number of partitions must be "
             "greater than 1.");
DEFINE_int32(table_num_range_partitions, 1,
             "The number of range partitions to create when this tool creates "
             "a new table. A range partition schema will be determined to "
             "evenly split a sequential workload across ranges, leaving "
             "the outermost ranges unbounded to ensure coverage of the entire "
             "keyspace. Note: The total number of partitions must be greater "
             "than 1.");
DEFINE_int32(table_num_replicas, 1,
             "The number of replicas for the auto-created table; "
             "0 means 'use server-side default'.");
DEFINE_bool(use_random, false,
            "Whether to use random numbers instead of sequential ones for both primary keys and "
            "non-primary key columns. In case of using random numbers collisions are "
            "possible over the data for columns with unique constraint (e.g. primary key). "
            "This option has been deprecated, use '--use_random_pk' and/or '--use_random_non_pk' "
            "instead. If either '--use_random_pk' or '--use_random_non_pk' is specified with "
            "'--use_random' then this option will be ignored.");
DEFINE_bool(use_random_pk, false,
            "Whether to use random numbers instead of sequential ones for primary key "
            "columns. Using random numbers may cause collisions over primary key columns.");
DEFINE_bool(use_random_non_pk, false,
            "Whether to use random numbers instead of sequential ones for non-primary key "
            "columns.");
DEFINE_bool(use_upsert, false,
            "Whether to use UPSERT instead of INSERT to store the generated "
            "data into the table");
DEFINE_bool(txn_start, false,
            "Whether the generated rows are inserted in the context of a "
            "multi-row transaction. For now, only supported if configured "
            "to insert rows (i.e. UPDATE and DELETE are not yet supported). "
            "Unless --txn_commit or --txn_rollback is specified, the started "
            "transaction is left 'as is' (i.e. neither commit, nor rollback "
            "is performed) once the tool completes its operations.");
DEFINE_bool(txn_commit, false,
            "Whether to commit the multi-row transaction which contains all "
            "the inserted rows. Setting --txn_commit=true implies setting "
            "--txn_start=true as well.");
DEFINE_bool(txn_rollback, false,
            "Whether to rollback the multi-row transaction which contains all "
            "the inserted rows. Setting --txn_rollback=true implies setting "
            "--txn_start=true as well.");

DECLARE_bool(show_values);
DECLARE_int32(num_threads);
DECLARE_int32(scan_batch_size);
DECLARE_string(replica_selection);
DECLARE_string(table_name);

namespace kudu {
namespace tools {

namespace {

bool ValidatePartitionFlags() {
  int num_tablets = FLAGS_table_num_hash_partitions * FLAGS_table_num_range_partitions;
  if (num_tablets < 1) {
    LOG(ERROR) << Substitute("Invalid partitioning: --table_num_hash_partitions=$0 "
                  "--table_num_range_partitions=$1, must specify more than one partition "
                  "for auto-generated tables", FLAGS_table_num_hash_partitions,
                  FLAGS_table_num_range_partitions);
    return false;
  }
  return true;
}
GROUP_FLAG_VALIDATOR(partition_flags, &ValidatePartitionFlags);

bool ValidateTxnFlags() {
  if ((FLAGS_txn_commit || FLAGS_txn_rollback || FLAGS_txn_start) &&
      FLAGS_use_upsert) {
    LOG(ERROR) << Substitute("only inserts are supported as transactional "
                             "operations for now: either remove/unset the "
                             "--use_upsert flag or remove/unset txn-related "
                             "flags");
    return false;
  }
  if (FLAGS_txn_commit && FLAGS_txn_rollback) {
    LOG(ERROR) << Substitute("both --txn_commit and --txn_rollback are set: "
                             "unset one of those to resolve the conflict");
    return false;
  }
  return true;
}
GROUP_FLAG_VALIDATOR(txn_flags, &ValidateTxnFlags);

const char* OpTypeToString(KuduWriteOperation::Type op_type) {
  switch (op_type) {
    case KuduWriteOperation::INSERT:
      return "INSERT";
    case KuduWriteOperation::DELETE:
      return "DELETE";
    case KuduWriteOperation::UPSERT:
      return "UPSERT";
    default:
      LOG(FATAL) << Substitute("unsupported op_type $0", op_type);
  }
}

class Generator {
 public:
  enum Mode {
    MODE_SEQ,
    MODE_RAND,
  };

  Generator(Mode m, int64_t seed, size_t string_len)
      : mode_(m),
        seq_(seed),
        random_(seed),
        string_len_(string_len) {
  }

  ~Generator() = default;

  uint64_t NextImpl() {
    if (mode_ == MODE_SEQ) {
      return seq_++;
    }
    return random_.Next64();
  }

  template <typename T>
  T Next() {
    return NextImpl() & numeric_limits<T>::max();
  }

 private:
  const Mode mode_;
  uint64_t seq_;
  Random random_;
  const size_t string_len_;
  string buf_;
};

template <>
bool Generator::Next() {
  return (NextImpl() & 0x1);
}

template <>
double Generator::Next() {
  return static_cast<double>(NextImpl());
}

template <>
float Generator::Next() {
  return static_cast<float>(NextImpl());
}

template <>
string Generator::Next() {
  buf_.clear();
  StrAppend(&buf_, NextImpl(), ".");
  // Truncate or extend with 'x's.
  buf_.resize(string_len_, 'x');
  return buf_;
}

// Utility function that determines the range of generated values each thread
// should insert across if inserting in non-random mode. In random mode, this
// is used to generate different RNG seeds per thread.
int64_t SpanPerThread(int num_key_columns) {
  CHECK_LT(0, num_key_columns);
  CHECK_LT(0, FLAGS_num_threads);
  const auto per_thread_limit = numeric_limits<int64_t>::max() /
      (num_key_columns * FLAGS_num_threads);
  return (FLAGS_num_rows_per_thread < 0 ||
          FLAGS_num_rows_per_thread > per_thread_limit)
      ? numeric_limits<int64_t>::max() / FLAGS_num_threads
      : FLAGS_num_rows_per_thread * num_key_columns;
}

Status GenerateRowData(Generator* key_gen, Generator* value_gen, KuduPartialRow* row,
                       const string& fixed_string, KuduWriteOperation::Type op_type) {
  const vector<ColumnSchema>& columns(row->schema()->columns());
  DCHECK(op_type == KuduWriteOperation::INSERT ||
         op_type == KuduWriteOperation::DELETE ||
         op_type == KuduWriteOperation::UPSERT);
  const size_t gen_column_count = op_type == KuduWriteOperation::DELETE
      ? row->schema()->num_key_columns()
      : columns.size();
  // Seperate key Generator and value Generator, so we can generate the same primary keys
  // when perform DELETE operations.
  Generator* gen = key_gen;
  for (size_t idx = 0; idx < gen_column_count; ++idx) {
    if (idx == row->schema()->num_key_columns()) {
      gen = value_gen;
    }
    const TypeInfo* tinfo = columns[idx].type_info();
    switch (tinfo->type()) {
      case BOOL:
        RETURN_NOT_OK(row->SetBool(idx, gen->Next<bool>()));
        break;
      case INT8:
        RETURN_NOT_OK(row->SetInt8(idx, gen->Next<int8_t>()));
        break;
      case INT16:
        RETURN_NOT_OK(row->SetInt16(idx, gen->Next<int16_t>()));
        break;
      case INT32:
        RETURN_NOT_OK(row->SetInt32(idx, gen->Next<int32_t>()));
        break;
      case INT64:
        RETURN_NOT_OK(row->SetInt64(idx, gen->Next<int64_t>()));
        break;
      case UNIXTIME_MICROS:
        RETURN_NOT_OK(row->SetUnixTimeMicros(idx, gen->Next<int64_t>()));
        break;
      case DATE:
        RETURN_NOT_OK(row->SetDate(idx, gen->Next<int32_t>()));
        break;
      case FLOAT:
        RETURN_NOT_OK(row->SetFloat(idx, gen->Next<float>()));
        break;
      case DOUBLE:
        RETURN_NOT_OK(row->SetDouble(idx, gen->Next<double>()));
        break;
      case DECIMAL32:
        RETURN_NOT_OK(row->SetUnscaledDecimal(idx, std::min(gen->Next<int32_t>(),
                                                            kMaxUnscaledDecimal32)));
        break;
      case DECIMAL64:
        RETURN_NOT_OK(row->SetUnscaledDecimal(idx, std::min(gen->Next<int64_t>(),
                                                            kMaxUnscaledDecimal64)));
        break;
      case DECIMAL128:
        RETURN_NOT_OK(row->SetUnscaledDecimal(idx, std::min(gen->Next<int128_t>(),
                                                            kMaxUnscaledDecimal128)));
        break;
      case BINARY:
        if (fixed_string.empty()) {
          RETURN_NOT_OK(row->SetBinary(idx, gen->Next<string>()));
        } else {
          RETURN_NOT_OK(row->SetBinaryNoCopy(idx, fixed_string));
        }
        break;
      case STRING:
        if (fixed_string.empty()) {
          RETURN_NOT_OK(row->SetString(idx, gen->Next<string>()));
        } else {
          RETURN_NOT_OK(row->SetStringNoCopy(idx, fixed_string));
        }
        break;
      case VARCHAR:
        if (fixed_string.empty()) {
          RETURN_NOT_OK(row->SetVarchar(idx, gen->Next<string>()));
        } else {
          RETURN_NOT_OK(row->SetStringNoCopy(idx, fixed_string));
        }
        break;
      default:
        return Status::InvalidArgument("unknown data type");
    }
  }
  return Status::OK();
}

mutex cerr_lock;

struct WriteResults {
  Status status;
  uint64_t row_count = 0;
  uint64_t err_count = 0;
  uint64_t latest_observed_timestamp = 0;
};

WriteResults GeneratorThread(const shared_ptr<KuduSession>& session,
                             const string& table_name,
                             size_t gen_idx,
                             KuduWriteOperation::Type op_type) {
  Generator::Mode key_gen_mode = Generator::MODE_SEQ;
  Generator::Mode value_gen_mode = Generator::MODE_SEQ;

  if (FLAGS_use_random_pk || FLAGS_use_random_non_pk) {
    // Honor the non-default values for new flags ignoring the old deprecated FLAGS_use_random.
    if (FLAGS_use_random_pk) {
      key_gen_mode = Generator::MODE_RAND;
    }
    if (FLAGS_use_random_non_pk) {
      value_gen_mode = Generator::MODE_RAND;
    }
  } else if (FLAGS_use_random) {
    key_gen_mode = value_gen_mode = Generator::MODE_RAND;
  }

  const size_t flush_per_n_rows = FLAGS_flush_per_n_rows;
  const uint64_t gen_seq_start = FLAGS_seq_start;
  auto* client = session->client();
  DCHECK(client);
  int64_t idx = 0;

  auto generator = [&]() {
    const int64_t num_rows_per_gen = FLAGS_num_rows_per_thread;
    if (num_rows_per_gen == 0) {
      return Status::OK();
    }
    RETURN_NOT_OK(session->SetMutationBufferFlushWatermark(
                     FLAGS_buffer_flush_watermark_pct));
    RETURN_NOT_OK(session->SetMutationBufferSpace(
                     FLAGS_buffer_size_bytes));
    RETURN_NOT_OK(session->SetMutationBufferMaxNum(FLAGS_buffers_num));
    RETURN_NOT_OK(session->SetErrorBufferSpace(FLAGS_error_buffer_size_bytes));
    RETURN_NOT_OK(session->SetFlushMode(
        flush_per_n_rows == 0 ? KuduSession::AUTO_FLUSH_BACKGROUND
                              : KuduSession::MANUAL_FLUSH));

    shared_ptr<KuduTable> table;
    RETURN_NOT_OK(client->OpenTable(table_name, &table));

    // Planning for non-intersecting ranges for different generator threads
    // in sequential generation mode.
    const int64_t gen_span = SpanPerThread(KuduSchema::ToSchema(
        table->schema()).num_key_columns());
    const int64_t gen_seed = gen_idx * gen_span + gen_seq_start;
    Generator key_gen(key_gen_mode, gen_seed, FLAGS_string_len);
    Generator value_gen(value_gen_mode, gen_seed, FLAGS_string_len);
    unique_ptr<KuduWriteOperation> op;
    for (; num_rows_per_gen < 0 || idx < num_rows_per_gen; ++idx) {
      switch (op_type) {
        case KuduWriteOperation::INSERT:
          op.reset(table->NewInsert());
          break;
        case KuduWriteOperation::DELETE:
          op.reset(table->NewDelete());
          break;
        case KuduWriteOperation::UPSERT:
          op.reset(table->NewUpsert());
          break;
        default:
          LOG(FATAL) << Substitute("unknown op_type $0", op_type);
      }
      RETURN_NOT_OK(GenerateRowData(
          &key_gen,
          op_type == KuduWriteOperation::DELETE ? nullptr : &value_gen,
          op->mutable_row(),
          FLAGS_string_fixed,
          op_type));
      RETURN_NOT_OK(session->Apply(op.release()));
      if (flush_per_n_rows != 0 && idx != 0 && idx % flush_per_n_rows == 0) {
        session->FlushAsync(nullptr);
      }
    }
    return session->Flush();
  };

  WriteResults results;
  Status s = generator();
  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  session->GetPendingErrors(&errors, nullptr);

  results.status = std::move(s);
  results.row_count = idx;
  results.err_count = errors.size();
  results.latest_observed_timestamp = client->GetLatestObservedTimestamp();

  if (!errors.empty() && FLAGS_show_first_n_errors > 0) {
    ostringstream str;
    str << "Error from generator " << std::setw(4) << gen_idx << ":" << endl;
    for (size_t i = 0; i < errors.size() && i < FLAGS_show_first_n_errors; ++i) {
      str << "  " << errors[i]->status().ToString() << endl;
    }
    // Serialize access to the stderr to prevent garbled output.
    lock_guard<mutex> lock(cerr_lock);
    cerr << str.str() << endl;
  }

  return results;
}

WriteResults GenerateWriteRows(const vector<shared_ptr<KuduSession>>& sessions,
                               const string& table_name,
                               KuduWriteOperation::Type op_type) {
  DCHECK(op_type == KuduWriteOperation::INSERT ||
         op_type == KuduWriteOperation::DELETE ||
         op_type == KuduWriteOperation::UPSERT);

  const size_t gen_num = sessions.size();
  vector<WriteResults> results(gen_num);
  vector<thread> threads;
  threads.reserve(gen_num);
  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();
  for (size_t i = 0; i < gen_num; ++i) {
    threads.emplace_back(
      [=, &results]() {
        results[i] = GeneratorThread(sessions[i], table_name, i, op_type);
      });
  }
  for (auto& t : threads) {
    t.join();
  }
  sw.stop();
  const double time_total_ms = sw.elapsed().wall_millis();
  WriteResults combined;
  for (auto& r : results) {
    combined.status = combined.status.ok() ? r.status : combined.status;
    combined.row_count += r.row_count;
    combined.err_count += r.err_count;
    combined.latest_observed_timestamp =
        std::max(combined.latest_observed_timestamp, r.latest_observed_timestamp);
  }
  cout << endl
       << OpTypeToString(op_type) << " report" << endl
       << "    rows total: " << combined.row_count << endl
       << "    time total: " << time_total_ms << " ms" << endl;
  if (combined.row_count != 0 && combined.err_count == 0) {
    // Report per-row timings only if there were no write errors, otherwise the
    // readings do not make much sense.
    cout << "  time per row: " << time_total_ms / combined.row_count << " ms" << endl;
  }

  // Make first non-OK error status, if any, as a result.
  if (!combined.status.ok() || combined.err_count != 0) {
    string err_str;
    if (!combined.status.ok()) {
      SubstituteAndAppend(&err_str, combined.status.ToString());
    }
    if (combined.err_count != 0) {
      if (!combined.status.ok()) {
        SubstituteAndAppend(&err_str, "; ");
      }
      SubstituteAndAppend(&err_str, "Encountered $0 write operation errors", combined.err_count);
    }
    combined.status = Status::RuntimeError(err_str);
  }

  return combined;
}

// Fetch all rows from the table with the specified name; iterate over them
// and output their total count.
Status CountTableRows(const shared_ptr<KuduClient>& client,
                      const string& table_name, uint64_t* count) {
  TableScanner scanner(client, table_name);
  scanner.SetReadMode(KuduScanner::ReadMode::READ_YOUR_WRITES);
  const auto& replica_selection_str = FLAGS_replica_selection;
  if (!replica_selection_str.empty()) {
    RETURN_NOT_OK(scanner.SetReplicaSelection(replica_selection_str));
  }
  RETURN_NOT_OK(scanner.StartScan());
  if (count != nullptr) {
    *count = scanner.TotalScannedCount();
  }

  return Status::OK();
}

Status TestLoadGenerator(const RunnerContext& context) {
  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));

  const size_t gen_num = FLAGS_num_threads;
  string table_name;
  bool is_auto_table = false;
  if (!FLAGS_table_name.empty()) {
    table_name = FLAGS_table_name;
  } else {
    constexpr const char* const kKeyColumnName = "key";

    // The auto-created table case.
    is_auto_table = true;
    ObjectIdGenerator oid_generator;
    table_name = Substitute("$0loadgen_auto_$1",
        FLAGS_auto_database.empty() ? "" : FLAGS_auto_database + ".",
        oid_generator.Next());
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn(kKeyColumnName)->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32);
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING);
    RETURN_NOT_OK(b.Build(&schema));

    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    table_creator->table_name(table_name).schema(&schema);
    if (FLAGS_table_num_replicas > 0) {
      table_creator->num_replicas(FLAGS_table_num_replicas);
    }
    if (FLAGS_table_num_range_partitions >= 1) {
      // Split the generated span for a sequential workload evenly across all
      // tablets. In case we're inserting in random mode, use unbounded range
      // partitioning, so the table has key coverage of the entire keyspace.
      const int64_t total_inserted_span =
          SpanPerThread(KuduSchema::ToSchema(schema).num_key_columns()) * gen_num;
      const int64_t span_per_range =
          total_inserted_span / FLAGS_table_num_range_partitions;
      table_creator->set_range_partition_columns({ kKeyColumnName });
      for (int i = 1; i < FLAGS_table_num_range_partitions; i++) {
        unique_ptr<KuduPartialRow> split(schema.NewRow());
        int64_t split_val = FLAGS_seq_start + i * span_per_range;
        RETURN_NOT_OK(split->SetInt64(kKeyColumnName, split_val));
        table_creator->add_range_partition_split(split.release());
      }
    }
    if (FLAGS_table_num_hash_partitions > 1) {
      table_creator->add_hash_partitions({ kKeyColumnName },
                                         FLAGS_table_num_hash_partitions);
    }
    RETURN_NOT_OK(table_creator->Create());
  }
  cout << "Using " << (is_auto_table ? "auto-created " : "")
       << "table '" << table_name << "'" << endl;

  const bool is_transactional =
      FLAGS_txn_start || FLAGS_txn_rollback || FLAGS_txn_commit;
  shared_ptr<KuduTransaction> txn;
  if (is_transactional) {
    RETURN_NOT_OK(client->NewTransaction(&txn));
  }

  // Create a session per generator thread. A KuduSession object keeps a
  // reference to its client handle, so there is no need to keep references
  // to the newly created client objects themselves.
  vector<shared_ptr<KuduSession>> sessions;
  sessions.reserve(gen_num);
  for (size_t i = 0; i < gen_num; ++i) {
    shared_ptr<KuduClient> c;
    if (FLAGS_use_client_per_thread) {
      RETURN_NOT_OK(CreateKuduClient(context, &c));
    } else {
      c = client;
    }
    if (is_transactional) {
      shared_ptr<KuduSession> session;
      RETURN_NOT_OK(txn->CreateSession(&session));
      sessions.emplace_back(std::move(session));
    } else {
      sessions.emplace_back(c->NewSession());
    }
  }

  WriteResults write_results = GenerateWriteRows(
      sessions,
      table_name,
      FLAGS_use_upsert ? KuduWriteOperation::UPSERT : KuduWriteOperation::INSERT);
  RETURN_NOT_OK(write_results.status);
  client->SetLatestObservedTimestamp(write_results.latest_observed_timestamp);
  if (txn) {
    if (FLAGS_txn_commit) {
      RETURN_NOT_OK(txn->Commit());
    } else if (FLAGS_txn_rollback) {
      RETURN_NOT_OK(txn->Rollback());
    }
  }
  if (FLAGS_run_scan) {
    // In case if no write errors encountered, run a table scan to make sure
    // the number of inserted rows matches the results of the scan. In case
    // of a transactional session, no rows are visible unless the transaction
    // has been committed. It's non-conventional, but 'dirty' reads are not
    // implemented for transactional sessions as of now: txn-related
    // functionality is targeting "batch insert" use cases only.
    uint64_t count = 0;
    RETURN_NOT_OK(CountTableRows(client, table_name, &count));
    const bool none_rows_visible =
        (FLAGS_txn_start && !FLAGS_txn_commit) || FLAGS_txn_rollback;
    const uint64_t expected_row_count =
        none_rows_visible ? 0 : write_results.row_count;
    cout << endl
         << "Scanner report" << endl
         << "  expected rows: " << expected_row_count << endl
         << "  actual rows  : " << count << endl;
    if (count != expected_row_count) {
      return Status::RuntimeError(Substitute(
          "Row count mismatch: expected $0, actual $1", write_results.row_count, count));
    }
  }

  if (FLAGS_run_cleanup) {
    RETURN_NOT_OK(GenerateWriteRows(
        sessions, table_name, KuduWriteOperation::DELETE).status);
  }

  if (is_auto_table && !FLAGS_keep_auto_table) {
    cout << "Dropping auto-created table '" << table_name << "'" << endl;
    // Drop the table which was automatically created to run the test.
    RETURN_NOT_OK(client->DeleteTable(table_name));
  }

  return Status::OK();
}

Status TableScan(const RunnerContext& context) {
  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));

  const string& table_name = FindOrDie(context.required_args, kTableNameArg);

  FLAGS_show_values = false;
  TableScanner scanner(client, table_name);
  scanner.SetOutput(&cout);
  scanner.SetScanBatchSize(FLAGS_scan_batch_size);
  const auto& replica_selection_str = FLAGS_replica_selection;
  if (!replica_selection_str.empty()) {
    RETURN_NOT_OK(scanner.SetReplicaSelection(replica_selection_str));
  }
  return scanner.StartScan();
}

Status TabletScan(const RunnerContext& context) {
  const string& tablet_id = FindOrDie(context.required_args, kTabletIdArg);

  // Initialize just enough of a tserver to bootstrap the tablet. We must
  // bootstrap so that our scan includes data from the WAL segments.
  //
  // Note: we need a read-write FsManager because bootstrapping will do
  // destructive things (e.g. rename the tablet's WAL segment directory).
  FsManager fs(Env::Default(), FsManagerOpts());
  RETURN_NOT_OK(fs.Open());

  scoped_refptr<TabletMetadata> tmeta;
  RETURN_NOT_OK(TabletMetadata::Load(&fs, tablet_id, &tmeta));

  scoped_refptr<ConsensusMetadataManager> cmeta_manager(
      new ConsensusMetadataManager(&fs));
  scoped_refptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK(cmeta_manager->Load(tablet_id, &cmeta));

  LogicalClock clock(Timestamp::kInitialTimestamp);
  RETURN_NOT_OK(clock.Init());

  scoped_refptr<LogAnchorRegistry> registry(new LogAnchorRegistry());

  // Bootstrap the tablet.
  std::shared_ptr<Tablet> tablet;
  scoped_refptr<Log> log;
  ConsensusBootstrapInfo cbi;
  RETURN_NOT_OK(tablet::BootstrapTablet(std::move(tmeta),
                                        cmeta->CommittedConfig(),
                                        &clock,
                                        /*mem_tracker=*/ nullptr,
                                        /*result_tracker=*/ nullptr,
                                        /*metric_registry=*/ nullptr,
                                        /*file_cache=*/ nullptr,
                                        /*tablet_replica=*/ nullptr,
                                        std::move(registry),
                                        &tablet,
                                        &log,
                                        &cbi));

  // Tablet has been bootstrapped and opened. We can now scan it.
  for (int i = 0; i < FLAGS_num_iters; i++) {
    LOG_TIMING(INFO, Substitute("scanning tablet (iter $0)", i)) {
      SchemaPtr projection_ptr =
          std::make_shared<Schema>(tablet->schema()->CopyWithoutColumnIds());
      Schema& projection = *projection_ptr;
      RowIteratorOptions opts;
      opts.projection = projection_ptr;
      opts.order = FLAGS_ordered_scan ? ORDERED : UNORDERED;
      unique_ptr<RowwiseIterator> iter;
      RETURN_NOT_OK(tablet->NewRowIterator(std::move(opts), &iter));
      RETURN_NOT_OK(iter->Init(nullptr));
      RowBlockMemory mem(1024);
      RowBlock block(&projection, 100, &mem);
      int64_t rows_scanned = 0;
      while (iter->HasNext()) {
        mem.Reset();
        RETURN_NOT_OK(iter->NextBlock(&block));
        rows_scanned += block.nrows();
        KLOG_EVERY_N_SECS(INFO, 10) << "scanned " << rows_scanned << " rows";
      }
      LOG(INFO) << "scanned " << rows_scanned << " rows";
    }
  }
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildPerfMode() {
  unique_ptr<Action> loadgen =
      ClusterActionBuilder("loadgen", &TestLoadGenerator)
      .Description("Run load generation with optional scan afterwards")
      .ExtraDescription(
          "Run load generation tool which inserts auto-generated data into "
          "an existing or auto-created table as fast as possible. "
          "If requested, also scan the inserted rows to check whether the "
          "actual count of inserted rows matches the expected one.")
      .AddOptionalParameter("auto_database")
      .AddOptionalParameter("buffer_flush_watermark_pct")
      .AddOptionalParameter("buffer_size_bytes")
      .AddOptionalParameter("buffers_num")
      .AddOptionalParameter("error_buffer_size_bytes")
      .AddOptionalParameter("flush_per_n_rows")
      .AddOptionalParameter("keep_auto_table")
      .AddOptionalParameter("num_rows_per_thread")
      .AddOptionalParameter("num_threads")
      .AddOptionalParameter("run_cleanup")
      .AddOptionalParameter("run_scan")
      .AddOptionalParameter("seq_start")
      .AddOptionalParameter("show_first_n_errors")
      .AddOptionalParameter("string_fixed")
      .AddOptionalParameter("string_len")
      .AddOptionalParameter(
          "table_name",
          boost::none,
          string("Name of an existing table to use for the test. The test will "
                 "determine the structure of the table schema and "
                 "populate it with data accordingly. If left empty, "
                 "the test automatically creates a table of pre-defined columnar "
                 "structure with unique name and uses it to insert "
                 "auto-generated data. The auto-created table is dropped "
                 "upon successful completion of the test if not overridden "
                 "by the '--keep_auto_table' flag. If running the test against "
                 "an already existing table, it's recommended to use a dedicated "
                 "table created just for testing purposes: the tool doesn't delete "
                 "the rows it inserted into the table. Neither the existing table "
                 "nor its data is ever dropped/deleted."))
      .AddOptionalParameter("table_num_hash_partitions")
      .AddOptionalParameter("table_num_range_partitions")
      .AddOptionalParameter("table_num_replicas")
      .AddOptionalParameter("txn_start")
      .AddOptionalParameter("txn_commit")
      .AddOptionalParameter("txn_rollback")
      .AddOptionalParameter("use_client_per_thread")
      .AddOptionalParameter("use_random")
      .AddOptionalParameter("use_random_pk")
      .AddOptionalParameter("use_random_non_pk")
      .AddOptionalParameter("use_upsert")
      .Build();

  unique_ptr<Action> table_scan =
      ClusterActionBuilder("table_scan", &TableScan)
      .Description("Show row count and scanning time cost of tablets in a table")
      .ExtraDescription("Show row count and scanning time of tablets in a table. "
          "This can be useful to check for row count skew across different tablets, "
          "or whether there is a long latency tail when scanning different tables.")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to scan"})
      .AddOptionalParameter("columns")
      .AddOptionalParameter("row_count_only")
      .AddOptionalParameter("report_scanner_stats")
      .AddOptionalParameter("scan_batch_size")
      .AddOptionalParameter("fill_cache")
      .AddOptionalParameter("num_threads")
      .AddOptionalParameter("predicates")
      .AddOptionalParameter("tablets")
      .AddOptionalParameter("replica_selection")
      .Build();

  // TODO(aserbin): move this to tool_local_replica.cc
  unique_ptr<Action> tablet_scan =
      ActionBuilder("tablet_scan", &TabletScan)
      .Description("Show row count of a local tablet")
      .AddRequiredParameter({ kTabletIdArg, kTabletIdArgDesc })
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("fs_metadata_dir")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("num_iters")
      .AddOptionalParameter("ordered_scan")
      .Build();
  return ModeBuilder("perf")
      .Description("Measure the performance of a Kudu cluster")
      .AddAction(std::move(loadgen))
      .AddAction(std::move(table_scan))
      .AddAction(std::move(tablet_scan))
      .Build();
}

} // namespace tools
} // namespace kudu
