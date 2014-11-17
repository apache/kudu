// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Simple tool to insert "random junk" rows into an arbitrary table.
// First column is in ascending order, the rest are random data.
// Helps make things like availability demos a little easier.

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <iostream>
#include <tr1/memory>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/common/row_util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/logging.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"

DEFINE_string(master_address, "localhost",
              "Comma separated list of master addresses to run against.");

namespace kudu {

using std::string;
using std::tr1::shared_ptr;
using std::vector;

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduInsert;
using client::KuduSchema;
using client::KuduSession;
using client::KuduTable;

void PrintUsage(char** argv) {
  std::cerr << "usage: " << argv[0] << " [--master_address localhost] <table_name>"
            << std::endl;
}

// Detect the type of the given column and coerce the given number value in
// 'value' to the data type of that column.
// At the time of this writing, we only support ints, bools, and strings.
// For the numbers / bool, the value is truncated to fit the data type.
// For the string, we encode the number as hex.
static void WriteValueToColumn(const KuduSchema& schema,
                               int col_idx,
                               uint64_t value,
                               KuduPartialRow* row) {
  KuduColumnSchema::DataType type = schema.Column(col_idx).type();
  char buf[kFastToBufferSize];
  switch (type) {
    case KuduColumnSchema::UINT8:
      CHECK_OK(row->SetUInt8(col_idx, value));
      break;
    case KuduColumnSchema::INT8:
      CHECK_OK(row->SetInt8(col_idx, value));
      break;
    case KuduColumnSchema::UINT16:
      CHECK_OK(row->SetUInt16(col_idx, value));
      break;
    case KuduColumnSchema::INT16:
      CHECK_OK(row->SetInt16(col_idx, value));
      break;
    case KuduColumnSchema::UINT32:
      CHECK_OK(row->SetUInt32(col_idx, value));
      break;
    case KuduColumnSchema::INT32:
      CHECK_OK(row->SetInt32(col_idx, value));
      break;
    case KuduColumnSchema::UINT64:
      CHECK_OK(row->SetUInt64(col_idx, value));
      break;
    case KuduColumnSchema::INT64:
      CHECK_OK(row->SetInt64(col_idx, value));
      break;
    case KuduColumnSchema::STRING:
      CHECK_OK(row->SetStringCopy(col_idx, FastHex64ToBuffer(value, buf)));
      break;
    case KuduColumnSchema::BOOL:
      CHECK_OK(row->SetBool(col_idx, value));
      break;
    default:
      LOG(FATAL) << "Unexpected data type: " << type;
  }
}

static int WriteRandomDataToTable(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 2) {
    PrintUsage(argv);
    return 1;
  }
  InitGoogleLoggingSafe(argv[0]);
  FLAGS_logtostderr = true;

  string table_name = argv[1];

  vector<string> addrs = strings::Split(FLAGS_master_address, ",");
  CHECK(!addrs.empty()) << "At least one master address must be specified!";

  // Set up client.
  LOG(INFO) << "Connecting to Kudu Master...";
  shared_ptr<KuduClient> client;
  CHECK_OK(KuduClientBuilder()
           .master_server_addrs(addrs)
           .default_select_master_timeout(MonoDelta::FromSeconds(5))
           .Build(&client));

  LOG(INFO) << "Opening table...";
  scoped_refptr<KuduTable> table;
  CHECK_OK(client->OpenTable(table_name, &table));
  KuduSchema schema = table->schema();

  shared_ptr<KuduSession> session = client->NewSession();
  session->SetTimeoutMillis(5000); // Time out after 5 seconds.
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  Random random(GetRandomSeed32());

  LOG(INFO) << "Inserting random rows...";
  for (uint64_t record_id = 0; true; ++record_id) {

    gscoped_ptr<KuduInsert> insert = table->NewInsert();
    KuduPartialRow* row = insert->mutable_row();

    for (int col_idx = 0; col_idx < schema.num_columns(); col_idx++) {
      // We randomly generate the inserted data, except for the first column,
      // which is always based on a monotonic "record id".
      uint64_t value;
      if (col_idx == 0) {
        value = record_id;
      } else {
        value = random.Next64();
      }
      WriteValueToColumn(schema, col_idx, value, row);
    }

    LOG(INFO) << "Inserting record: " << DebugPartialRowToString(*row);
    CHECK_OK(session->Apply(insert.Pass()));
    Status s = session->Flush();
    if (PREDICT_FALSE(!s.ok())) {
      std::vector<client::KuduError*> errors;
      ElementDeleter d(&errors);
      bool overflow;
      session->GetPendingErrors(&errors, &overflow);
      CHECK(!overflow);
      BOOST_FOREACH(const client::KuduError* e, errors) {
        if (e->status().IsAlreadyPresent()) {
          LOG(WARNING) << "Ignoring insert error: " << e->status().ToString();
        } else {
          LOG(FATAL) << "Unexpected insert error: " << e->status().ToString();
        }
      }
      continue;
    }
    LOG(INFO) << "OK";
  }

  return 0;
}

} // namespace kudu

int main(int argc, char** argv) {
  return kudu::WriteRandomDataToTable(argc, argv);
}
