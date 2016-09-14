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
//   kudu test loadgen \
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
//   kudu test loadgen \
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
//   kudu test loadgen \
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
//   kudu test loadgen \
//     --run_scan=true \
//     127.0.0.1
//

#include "kudu/tools/tool_action.h"

#include <cstdint>
#include <cstdlib>
#include <ctime>

#include <algorithm>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/client/client.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/random.h"
#include "kudu/util/stopwatch.h"

using std::accumulate;
using std::cerr;
using std::cout;
using std::endl;
using std::numeric_limits;
using std::string;
using std::ostringstream;
using std::thread;
using std::vector;
using std::unique_ptr;

using kudu::ColumnSchema;
using kudu::KuduPartialRow;
using kudu::Stopwatch;
using kudu::TypeInfo;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduRowResult;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;

DEFINE_double(buffer_flush_watermark_pct, 0.5,
              "Mutation buffer flush watermark, in percentage of total size.");
DEFINE_int32(buffer_size_bytes, 4 * 1024 * 1024,
             "Size of the mutation buffer, per session (bytes).");
DEFINE_int32(buffers_num, 2,
             "Number of mutation buffers per session.");
DEFINE_int32(flush_per_n_rows, 0,
             "Perform async flush per given number of rows added. "
             "Setting to non-zero implicitly turns on manual flush mode.");
DEFINE_bool(keep_auto_table, false,
            "If using the auto-generated table, enabling this option "
            "retains the table populated with the data after the test "
            "finishes. By default, the auto-generated table is dropped "
            "after sucessfully finishing the test. NOTE: this parameter "
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
DEFINE_int32(show_first_n_errors, 0,
             "Output detailed information on the specified number of "
             "first n errors (if any).");
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
DEFINE_bool(use_random, false,
            "Whether to use random numbers instead of sequential ones. "
            "In case of using random numbers collisions are possible over "
            "the data for columns with unique constraint (e.g. primary key).");

namespace kudu {
namespace tools {


namespace {

const char* const kMasterAddressesArg = "master_addresses";

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
  ostringstream ss;
  ss << NextImpl() << ".";
  string str(ss.str());
  if (str.size() >= string_len_) {
    str = str.substr(0, string_len_);
  } else {
    str += string(string_len_ - str.size(), 'x');
  }
  return str;
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

void GeneratorThread(
    const shared_ptr<KuduClient>& client, const string& table_name,
    Generator::Mode gen_mode, int64_t gen_seed,
    uint64_t* row_count, uint64_t* err_count) {

  const size_t flush_per_n_rows = FLAGS_flush_per_n_rows;

  shared_ptr<KuduSession> session(client->NewSession());
  CHECK_OK(session->SetMutationBufferFlushWatermark(
      FLAGS_buffer_flush_watermark_pct));
  CHECK_OK(session->SetMutationBufferSpace(FLAGS_buffer_size_bytes));
  CHECK_OK(session->SetMutationBufferMaxNum(FLAGS_buffers_num));
  CHECK_OK(session->SetFlushMode(
      flush_per_n_rows == 0 ? KuduSession::AUTO_FLUSH_BACKGROUND
                            : KuduSession::MANUAL_FLUSH));

  shared_ptr<KuduTable> table;
  CHECK_OK(client->OpenTable(table_name, &table));

  Generator gen(gen_mode, gen_seed, FLAGS_string_len);
  uint64_t idx = 0;
  const size_t num_rows_per_thread = FLAGS_num_rows_per_thread;
  for (; num_rows_per_thread == 0 || idx < num_rows_per_thread;
       ++idx) {
    unique_ptr<KuduInsert> insert_op(table->NewInsert());
    CHECK_OK(GenerateRowData(&gen, insert_op->mutable_row(), FLAGS_string_fixed));
    CHECK_OK(session->Apply(insert_op.release()));
    if (flush_per_n_rows != 0 && idx != 0 && idx % flush_per_n_rows == 0) {
      session->FlushAsync(nullptr);
    }
  }
  CHECK_OK(session->Flush());
  if (row_count != nullptr) {
    *row_count = idx;
  }
  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  session->GetPendingErrors(&errors, nullptr);
  if (err_count != nullptr) {
    *err_count = errors.size();
  }
  for (size_t i = 0; i < errors.size() && i < FLAGS_show_first_n_errors; ++i) {
    cerr << errors[i]->status().ToString() << endl;
  }
}

void GenerateInsertRows(const shared_ptr<KuduClient>& client,
                        const string& table_name,
                        uint64_t* total_row_count, uint64_t* total_err_count) {

  const size_t num_threads = FLAGS_num_threads;
  const Generator::Mode generator_mode = FLAGS_use_random ? Generator::MODE_RAND
                                                          : Generator::MODE_SEQ;
  vector<uint64_t> row_count(num_threads, 0);
  vector<uint64_t> err_count(num_threads, 0);
  vector<thread> threads;
  // The 'seed span' allows to have non-intersecting ranges for column values
  // in sequential generation mode.
  const int64_t seed_span = numeric_limits<int64_t>::max() / num_threads;
  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back(&GeneratorThread, client, table_name, generator_mode,
                         seed_span * i, &row_count[i], &err_count[i]);
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
    static const Schema kSchema = Schema(
        {
          ColumnSchema(kKeyColumnName, INT64),
          ColumnSchema("int32_val", INT32),
          ColumnSchema("string_val", STRING),
          ColumnSchema("binary_val", BINARY),
        }, 1);

    // The auto-created table case.
    is_auto_table = true;
    ObjectIdGenerator oid_generator;
    table_name = "loadgen_auto_" + oid_generator.Next();
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32);
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING);
    RETURN_NOT_OK(b.Build(&schema));

    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    RETURN_NOT_OK(table_creator->table_name(table_name)
                  .schema(&schema)
                  .num_replicas(1)
                  .add_hash_partitions(vector<string>({ kKeyColumnName }), 8)
                  .wait(true)
                  .Create());
  }
  cout << "Using " << (is_auto_table ? "auto-created " : "")
       << "table '" << table_name << "'" << endl;

  uint64_t total_row_count = 0;
  uint64_t total_err_count = 0;
  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();
  GenerateInsertRows(client, table_name, &total_row_count, &total_err_count);
  sw.stop();
  const double total = sw.elapsed().wall_millis();
  cout << endl << "Generator report" << endl
       << "  time total  : " << total << " ms" << endl;
  if (total_row_count != 0) {
    cout << "  time per row: " << total / total_row_count << " ms" << endl;
  }
  if (total_err_count != 0) {
    return Status::RuntimeError(strings::Substitute("Encountered $0 errors",
                                                    total_err_count));
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
            strings::Substitute("Row count mismatch: expected $0, actual $1",
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

unique_ptr<Mode> BuildTestMode() {
  unique_ptr<Action> insert =
      ActionBuilder("loadgen", &TestLoadGenerator)
      .Description("Run load generation test with optional scan afterwards")
      .ExtraDescription(
          "Run load generation tool which inserts auto-generated data "
          "into already existing table as fast as possible. If requested, "
          "also run scan over the inserted rows to check whether the reported "
          "count or inserted rows matches with the expected one. "
          "NOTE: it's highly recommended to create a separate table for that "
          "because the tool does not clean the inserted data.")
      .AddRequiredParameter({ kMasterAddressesArg,
          "Comma-separated list of master addresses to run against. "
          "Addresses are in 'hostname:port' form where port may be omitted "
          "if a master server listens at the default port." })
      .AddOptionalParameter("buffer_flush_watermark_pct")
      .AddOptionalParameter("buffer_size_bytes")
      .AddOptionalParameter("buffers_num")
      .AddOptionalParameter("flush_per_n_rows")
      .AddOptionalParameter("keep_auto_table")
      .AddOptionalParameter("num_rows_per_thread")
      .AddOptionalParameter("num_threads")
      .AddOptionalParameter("run_scan")
      .AddOptionalParameter("string_fixed")
      .AddOptionalParameter("string_len")
      .AddOptionalParameter("table_name")
      .AddOptionalParameter("use_random")
      .Build();

  return ModeBuilder("test")
      .Description("Run various tests against a Kudu cluster")
      .AddAction(std::move(insert))
      .Build();
}

} // namespace tools
} // namespace kudu
