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
// This is a small load generation tool which pushes data to a tablet
// server as fast as possible. The table is supposed to be created already,
// and this tool populates it with generated data. As an option, it's possible
// to run a post-scan over the inserted rows to get total table row count
// as reported by the scan operation.
//
// See below for examples of usage.
//
// Run in MANUAL_FLUSH mode, 1 thread inserting 8M rows into auto-created table,
// flushing every 2000 rows, unlimited number of buffers with 32MB
// limit on their total size, with auto-generated strings
// of length 64 for binary and string fields
// with Kudu master server listening on the default port at localhost:
//
//   kudu perf loadgen \
//     --num_threads=1 \
//     --num_rows_per_thread=8000000 \
//     --string_len=64 \
//     --buffer_size_bytes=33554432 \
//     --buffers_num=0 \
//     --flush_per_n_rows=2000 \
//     127.0.0.1
//
//
// Run in AUTO_FLUSH_BACKGROUND mode, 2 threads inserting 4M rows each inserting
// into auto-created table, with limit of 8 buffers max 1MB in size total,
// having 12.5% for buffer flush watermark,
// using the specified pre-set string for binary and string fields
// with Kudu master server listening on the default port at localhost:
//
//   kudu perf loadgen \
//     --num_threads=2 \
//     --num_rows_per_thread=4000000 \
//     --string_fixed=012345678901234567890123456789012 \
//     --buffer_size_bytes=1048576 \
//     --buffer_flush_watermark_pct=0.125 \
//     --buffers_num=8 \
//     127.0.0.1
//
//
// Run in AUTO_FLUSH_BACKGROUND mode, 4 threads inserting 2M rows each inserting
// into auto-created table, with limit of 4 buffers max 64KB in size total,
// having 25% for buffer flush watermark,
// using the specified pre-set string for binary and string fields
// with Kudu master server listening at 127.0.0.1:8765
//
//   kudu perf loadgen \
//     --num_threads=4 \
//     --num_rows_per_thread=2000000 \
//     --string_fixed=0123456789 \
//     --buffer_size_bytes=65536 \
//     --buffers_num=4 \
//     --buffer_flush_watermark_pct=0.25 \
//     --table_name=bench_02 \
//     127.0.0.1:8765
//
//
// Run with default parameter values for data generation and batching,
// inserting data into auto-created table,
// with Kudu master server listening on the default port at localhost,
// plus run post-insertion row scan to verify
// that the count of the inserted rows matches the expected number:
//
//   kudu perf loadgen \
//     --run_scan=true \
//     127.0.0.1
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

#include "kudu/tools/tool_action.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/int128.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

using kudu::ColumnSchema;
using kudu::KuduPartialRow;
using kudu::Stopwatch;
using kudu::TypeInfo;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using std::accumulate;
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
            "(see the '--table_name' flag): the existing tables nor their data "
            "are never dropped/deleted.");
DEFINE_uint64(num_rows_per_thread, 1000,
              "Number of rows each thread generates and inserts; "
              "0 means unlimited. All rows generated by a thread are inserted "
              "in the context of the same session.");
DEFINE_int32(num_threads, 2,
             "Number of generator threads to run. Each thread runs its own "
             "KuduSession.");
DEFINE_bool(run_scan, false,
            "Whether to run post-insertion scan to verify that the count of "
            "the inserted rows matches the expected number. If enabled, "
            "the scan is run only if no errors were encountered "
            "while inserting the generated rows.");
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
DEFINE_string(table_name, "",
              "Name of an existing table to use for the test. The test will "
              "determine the structure of the table schema and "
              "populate it with data accordingly. If left empty, "
              "the test automatically creates a table of pre-defined columnar "
              "structure with unique name and uses it to insert "
              "auto-generated data. The auto-created table is dropped "
              "upon successful completion of the test if not overridden "
              "by the '--keep_auto_table' flag. If running the test against "
              "an already existing table, it's highly recommended to use a "
              "dedicated table created just for testing purposes: "
              "the existing table nor its data is never dropped/deleted.");
DEFINE_int32(table_num_buckets, 8,
             "The number of buckets to create when this tool creates a new table.");
DEFINE_int32(table_num_replicas, 1,
             "The number of replicas for the auto-created table; "
             "0 means 'use server-side default'.");
DEFINE_bool(use_random, false,
            "Whether to use random numbers instead of sequential ones. "
            "In case of using random numbers collisions are possible over "
            "the data for columns with unique constraint (e.g. primary key).");

namespace kudu {
namespace tools {

namespace {

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

Status GenerateRowData(Generator* gen, KuduPartialRow* row,
                       const string& fixed_string) {
  const vector<ColumnSchema>& columns(row->schema()->columns());
  for (size_t idx = 0; idx < columns.size(); ++idx) {
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
      default:
        return Status::InvalidArgument("unknown data type");
    }
  }
  return Status::OK();
}

mutex cerr_lock;

void GeneratorThread(
    const shared_ptr<KuduClient>& client, const string& table_name,
    size_t gen_idx, size_t gen_num,
    Status* status, uint64_t* row_count, uint64_t* err_count) {

  const Generator::Mode gen_mode = FLAGS_use_random ? Generator::MODE_RAND
                                                    : Generator::MODE_SEQ;
  const size_t flush_per_n_rows = FLAGS_flush_per_n_rows;
  const uint64_t gen_seq_start = FLAGS_seq_start;
  shared_ptr<KuduSession> session(client->NewSession());
  uint64_t idx = 0;

  auto generator = [&]() -> Status {
    RETURN_NOT_OK(session->SetMutationBufferFlushWatermark(
                     FLAGS_buffer_flush_watermark_pct));
    RETURN_NOT_OK(session->SetMutationBufferSpace(
                     FLAGS_buffer_size_bytes));
    RETURN_NOT_OK(session->SetMutationBufferMaxNum(FLAGS_buffers_num));
    RETURN_NOT_OK(session->SetErrorBufferSpace(FLAGS_error_buffer_size_bytes));
    RETURN_NOT_OK(session->SetFlushMode(
        flush_per_n_rows == 0 ? KuduSession::AUTO_FLUSH_BACKGROUND
                              : KuduSession::MANUAL_FLUSH));
    const size_t num_rows_per_gen = FLAGS_num_rows_per_thread;

    shared_ptr<KuduTable> table;
    RETURN_NOT_OK(client->OpenTable(table_name, &table));
    const size_t num_columns = table->schema().num_columns();

    // Planning for non-intersecting ranges for different generator threads
    // in sequential generation mode.
    const int64_t gen_span =
        (num_rows_per_gen == 0) ? numeric_limits<int64_t>::max() / gen_num
                                : num_rows_per_gen * num_columns;
    const int64_t gen_seed = gen_idx * gen_span + gen_seq_start;
    Generator gen(gen_mode, gen_seed, FLAGS_string_len);
    for (; num_rows_per_gen == 0 || idx < num_rows_per_gen; ++idx) {
      unique_ptr<KuduInsert> insert_op(table->NewInsert());
      RETURN_NOT_OK(GenerateRowData(&gen, insert_op->mutable_row(),
                                   FLAGS_string_fixed));
      RETURN_NOT_OK(session->Apply(insert_op.release()));
      if (flush_per_n_rows != 0 && idx != 0 && idx % flush_per_n_rows == 0) {
        session->FlushAsync(nullptr);
      }
    }
    RETURN_NOT_OK(session->Flush());

    return Status::OK();
  };

  *status = generator();
  if (row_count != nullptr) {
    *row_count = idx;
  }
  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  session->GetPendingErrors(&errors, nullptr);
  if (err_count != nullptr) {
    *err_count = errors.size();
  }
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
}

Status GenerateInsertRows(const shared_ptr<KuduClient>& client,
                          const string& table_name,
                          uint64_t* total_row_count,
                          uint64_t* total_err_count) {

  const size_t gen_num = FLAGS_num_threads;
  vector<Status> status(gen_num);
  vector<uint64_t> row_count(gen_num, 0);
  vector<uint64_t> err_count(gen_num, 0);
  vector<thread> threads;
  for (size_t i = 0; i < gen_num; ++i) {
    threads.emplace_back(&GeneratorThread, client, table_name, i, gen_num,
                         &status[i], &row_count[i], &err_count[i]);
  }
  for (auto& t : threads) {
    t.join();
  }
  if (total_row_count != nullptr) {
    *total_row_count = accumulate(row_count.begin(), row_count.end(), 0UL);
  }
  if (total_err_count != nullptr) {
    *total_err_count = accumulate(err_count.begin(), err_count.end(), 0UL);
  }
  // Return first non-OK error status, if any, as a result.
  const auto it = find_if(status.begin(), status.end(),
                          [&](const Status& s) { return !s.ok(); });
  if (it != status.end()) {
    return *it;
  }
  return Status::OK();
}

// Fetch all rows from the table with the specified name; iterate over them
// and output their total count.
Status CountTableRows(const shared_ptr<KuduClient>& client,
                      const string& table_name, uint64_t* count) {
  // It's assumed that all writing activity has stopped at this point.
  const uint64_t snapshot_timestamp = client->GetLatestObservedTimestamp();

  shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(table_name, &table));

  // It's necessary to have read-what-you-write behavior here. Since
  // tablet leader can change and there might be replica propagation delays,
  // set the snapshot to the latest observed one to get correct row count.
  // Due to KUDU-1656, there might be timeouts due to tservers catching up with
  // the requested snapshot. The simple workaround: if the timeout error occurs,
  // retry the row count operation.
  Status row_count_status;
  uint64_t row_count = 0;
  for (size_t i = 0; i < 3; ++i) {
    KuduScanner scanner(table.get());
    // NOTE: +1 is due to the current implementation of the scanner.
    RETURN_NOT_OK(scanner.SetSnapshotRaw(snapshot_timestamp + 1));
    RETURN_NOT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
    RETURN_NOT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
    row_count_status = scanner.Open();
    if (!row_count_status.ok()) {
      if (row_count_status.IsTimedOut()) {
        // Retry condition: start the row count over again.
        continue;
      }
      return row_count_status;
    }
    row_count = 0;
    while (scanner.HasMoreRows()) {
      KuduScanBatch batch;
      row_count_status = scanner.NextBatch(&batch);
      if (!row_count_status.ok()) {
        if (row_count_status.IsTimedOut()) {
          // Retry condition: start the row count over again.
          break;
        }
        return row_count_status;
      }
      row_count += batch.NumRows();
    }
    if (row_count_status.ok()) {
      // If it reaches this point with success,
      // stop the retry cycle since the result is ready.
      break;
    }
  }
  RETURN_NOT_OK(row_count_status);
  if (count != nullptr) {
    *count = row_count;
  }

  return Status::OK();
}

Status TestLoadGenerator(const RunnerContext& context) {
  const string& master_addresses_str =
      FindOrDie(context.required_args, kMasterAddressesArg);

  vector<string> master_addrs(strings::Split(master_addresses_str, ","));
  if (master_addrs.empty()) {
    return Status::InvalidArgument(
        "At least one master address must be specified");
  }
  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(KuduClientBuilder()
                .master_server_addrs(master_addrs)
                .Build(&client));
  string table_name;
  bool is_auto_table = false;
  if (!FLAGS_table_name.empty()) {
    table_name = FLAGS_table_name;
  } else {
    static const string kKeyColumnName = "key";

    // The auto-created table case.
    is_auto_table = true;
    ObjectIdGenerator oid_generator;
    table_name = "loadgen_auto_" + oid_generator.Next();
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn(kKeyColumnName)->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32);
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING);
    RETURN_NOT_OK(b.Build(&schema));

    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    RETURN_NOT_OK(table_creator->table_name(table_name)
                  .schema(&schema)
                  .num_replicas(FLAGS_table_num_replicas)
                  .add_hash_partitions(vector<string>({ kKeyColumnName }),
                                       FLAGS_table_num_buckets)
                  .wait(true)
                  .Create());
  }
  cout << "Using " << (is_auto_table ? "auto-created " : "")
       << "table '" << table_name << "'" << endl;

  uint64_t total_row_count = 0;
  uint64_t total_err_count = 0;
  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();
  Status status = GenerateInsertRows(client, table_name,
                                     &total_row_count, &total_err_count);
  sw.stop();
  const double total = sw.elapsed().wall_millis();
  cout << endl << "Generator report" << endl
       << "  time total  : " << total << " ms" << endl;
  if (total_row_count != 0) {
    cout << "  time per row: " << total / total_row_count << " ms" << endl;
  }
  if (!status.ok() || total_err_count != 0) {
    string err_str;
    if (!status.ok()) {
      SubstituteAndAppend(&err_str, status.ToString());
    }
    if (total_err_count != 0) {
      if (!status.ok()) {
        SubstituteAndAppend(&err_str,  "; ");
      }
      SubstituteAndAppend(&err_str, "Encountered $0 write operation errors",
                          total_err_count);
    }
    return Status::RuntimeError(err_str);
  }

  if (FLAGS_run_scan) {
    // Run a table scan to count inserted rows.
    uint64_t count;
    RETURN_NOT_OK(CountTableRows(client, table_name, &count));
    cout << endl << "Scanner report" << endl
         << "  expected rows: " << total_row_count << endl
         << "  actual rows  : " << count << endl;
    if (count != total_row_count) {
      return Status::RuntimeError(
            Substitute("Row count mismatch: expected $0, actual $1",
                       total_row_count, count));
    }
  }

  if (is_auto_table && !FLAGS_keep_auto_table) {
    cout << "Dropping auto-created table '" << table_name << "'" << endl;
    // Drop the table which was automatically created to run the test.
    RETURN_NOT_OK(client->DeleteTable(table_name));
  }

  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildPerfMode() {
  unique_ptr<Action> insert =
      ActionBuilder("loadgen", &TestLoadGenerator)
      .Description("Run load generation with optional scan afterwards")
      .ExtraDescription(
          "Run load generation tool which inserts auto-generated data into "
          "an existing or auto-created table as fast as possible. "
          "If requested, also scan the inserted rows to check whether the "
          "actual count of inserted rows matches the expected one.")
      .AddRequiredParameter({ kMasterAddressesArg,
          "Comma-separated list of master addresses to run against. "
          "Addresses are in 'hostname:port' form where port may be omitted "
          "if a master server listens at the default port." })
      .AddOptionalParameter("buffer_flush_watermark_pct")
      .AddOptionalParameter("buffer_size_bytes")
      .AddOptionalParameter("buffers_num")
      .AddOptionalParameter("error_buffer_size_bytes")
      .AddOptionalParameter("flush_per_n_rows")
      .AddOptionalParameter("keep_auto_table")
      .AddOptionalParameter("num_rows_per_thread")
      .AddOptionalParameter("num_threads")
      .AddOptionalParameter("run_scan")
      .AddOptionalParameter("seq_start")
      .AddOptionalParameter("show_first_n_errors")
      .AddOptionalParameter("string_fixed")
      .AddOptionalParameter("string_len")
      .AddOptionalParameter("table_name")
      .AddOptionalParameter("table_num_buckets")
      .AddOptionalParameter("table_num_replicas")
      .AddOptionalParameter("use_random")
      .Build();

  return ModeBuilder("perf")
      .Description("Measure the performance of a Kudu cluster")
      .AddAction(std::move(insert))
      .Build();
}

} // namespace tools
} // namespace kudu
