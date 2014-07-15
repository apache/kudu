// Copyright (c) 2014, Cloudera, inc.
//
// Simple tool to send an CREATE TABLE request for one of the demo tablets.
// This will eventually be replaced by a proper shell -- just a quick
// hack for easy demo purposes.

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <iostream>
#include <tr1/memory>

#include "benchmarks/tpch/tpch-schemas.h"
#include "benchmarks/ycsb-schema.h"
#include "client/client.h"
#include "tserver/tserver.pb.h"
#include "tserver/tserver_service.proxy.h"
#include "twitter-demo/twitter-schema.h"
#include "util/env.h"
#include "util/faststring.h"
#include "util/logging.h"

using std::string;
using std::tr1::shared_ptr;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduSchema;
using kudu::rpc::RpcController;

DEFINE_string(master_address, "localhost",
              "Address of master run against");

static const char* const kTwitterTabletId = "twitter";
static const char* const kTPCH1TabletId = "tpch1";
static const char* const kYCSBTabletId = "ycsb";

namespace kudu {

void PrintUsage(char** argv) {
  std::cerr << "usage: " << argv[0] << " <table name>" << std::endl;
}

string LoadFile(const string& path) {
  faststring buf;
  CHECK_OK(ReadFileToString(Env::Default(), path, &buf));
  return buf.ToString();
}

// TODO: refactor this and the associated constants into some sort of
// demo-tables.h class in a src/demos/ directory.
Status GetDemoSchema(const string& table_name, KuduSchema* schema) {
  if (table_name == kTwitterTabletId) {
    *schema = twitter_demo::CreateTwitterSchema();
  } else if (table_name == kTPCH1TabletId) {
    *schema = tpch::CreateLineItemSchema();
  } else if (table_name == kYCSBTabletId) {
    *schema = kudu::CreateYCSBSchema();
  } else {
    return Status::InvalidArgument("Invalid demo table name", table_name);
  }
  return Status::OK();
}

static int CreateDemoTable(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 2) {
    PrintUsage(argv);
    return 1;
  }
  InitGoogleLoggingSafe(argv[0]);
  FLAGS_logtostderr = true;

  string table_name = argv[1];

  KuduSchema schema;
  CHECK_OK(GetDemoSchema(table_name, &schema));

  // Set up client.
  shared_ptr<KuduClient> client;
  CHECK_OK(KuduClientBuilder()
           .master_server_addr(FLAGS_master_address)
           .Build(&client));

  CHECK_OK(client->NewTableCreator()
           ->table_name(table_name)
           .schema(&schema)
           .Create());
  return 0;
}

} // namespace kudu

int main(int argc, char** argv) {
  return kudu::CreateDemoTable(argc, argv);
}
