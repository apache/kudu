// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Tool to query tablet server operational data

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <tr1/memory>
#include <iostream>
#include <strstream>

#include "kudu/client/row_result.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"

using std::ostringstream;
using std::string;
using std::tr1::shared_ptr;
using std::vector;
using kudu::client::KuduRowResult;
using kudu::client::KuduScanner;
using kudu::tablet::TabletStatusPB;
using kudu::tserver::TabletServerAdminServiceProxy;
using kudu::tserver::TabletServerServiceProxy;
using kudu::tserver::DeleteTabletRequestPB;
using kudu::tserver::DeleteTabletResponsePB;
using kudu::tserver::ListTabletsRequestPB;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::NewScanRequestPB;
using kudu::tserver::ScanRequestPB;
using kudu::tserver::ScanResponsePB;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::server::ServerStatusPB;
using kudu::HostPort;
using kudu::Sockaddr;

const char* const kListTabletsOp = "list_tablets";
const char* const kAreTabletsRunningOp = "are_tablets_running";
const char* const kSetFlagOp = "set_flag";
const char* const kDumpTabletOp = "dump_tablet";
const char* const kDeleteTabletOp = "delete_tablet";
const char* const kCurrentTimestamp = "current_timestamp";
const char* const kStatus = "status";

DEFINE_string(server_address, "localhost",
              "Address of server to run against");
DEFINE_int64(timeout_ms, 1000 * 60, "RPC timeout in milliseconds");

DEFINE_bool(force, false, "If true, allows the set_flag command to set a flag "
            "which is not explicitly marked as runtime-settable. Such flag changes may be "
            "simply ignored on the server, or may cause the server to crash.");

// Check that the value of argc matches what's expected, otherwise return a
// non-zero exit code. Should be used in main().
#define CHECK_ARGC_OR_RETURN_WITH_USAGE(expected) \
  do { \
    if (argc != (expected)) { \
      google::ShowUsageWithFlagsRestrict(argv[0], __FILE__); \
      return 2; \
    } \
  } while (0);

// Invoke 'to_call' and check its result. If it failed, print 'to_prepend' and
// the error to cerr and return a non-zero exit code. Should be used in main().
#define RETURN_NOT_OK_PREPEND_FROM_MAIN(to_call, to_prepend) \
  do { \
    ::kudu::Status s = (to_call); \
    if (!s.ok()) { \
      std::cerr << (to_prepend) << ": " << s.ToString() << std::endl; \
      return 1; \
    } \
  } while (0);

namespace kudu {
namespace tools {

typedef ListTabletsResponsePB::StatusAndSchemaPB StatusAndSchemaPB;

class TsAdminClient {
 public:
  // Creates an admin client for host/port combination e.g.,
  // "localhost" or "127.0.0.1:7050".
  TsAdminClient(const std::string& addr, int64_t timeout_millis);

  // Initialized the client and connects to the specified tablet
  // server.
  Status Init();

  // Sets 'tablets' a list of status information for all tablets on a
  // given tablet server.
  Status ListTablets(std::vector<StatusAndSchemaPB>* tablets);


  // Sets the gflag 'flag' to 'val' on the remote server via RPC.
  // If 'force' is true, allows setting flags even if they're not marked as
  // safe to change at runtime.
  Status SetFlag(const string& flag, const string& val,
                 bool force);

  // Get the schema for the given tablet.
  Status GetTabletSchema(const std::string& tablet_id, SchemaPB* schema);

  // Dump the contents of the given tablet, in key order, to the console.
  Status DumpTablet(const std::string& tablet_id);

  // Delete a replica. The 'reason' string is passed to the tablet server,
  // used for logging.
  Status DeleteTablet(const std::string& tablet_id,
                      const std::string& reason);

  // Sets timestamp to the value of the tablet server's current timestamp.
  Status CurrentTimestamp(uint64_t* timestamp);

  // Get the server status
  Status GetStatus(ServerStatusPB* pb);
 private:
  std::string addr_;
  vector<Sockaddr> addrs_;
  MonoDelta timeout_;
  bool initted_;
  shared_ptr<server::GenericServiceProxy> generic_proxy_;
  gscoped_ptr<tserver::TabletServerServiceProxy> ts_proxy_;
  gscoped_ptr<tserver::TabletServerAdminServiceProxy> ts_admin_proxy_;
  shared_ptr<rpc::Messenger> messenger_;

  DISALLOW_COPY_AND_ASSIGN(TsAdminClient);
};

TsAdminClient::TsAdminClient(const string& addr, int64_t timeout_millis)
    : addr_(addr),
      timeout_(MonoDelta::FromMilliseconds(timeout_millis)),
      initted_(false) {
}

Status TsAdminClient::Init() {
  CHECK(!initted_);

  HostPort host_port;
  RETURN_NOT_OK(host_port.ParseString(addr_, tserver::TabletServer::kDefaultPort));
  MessengerBuilder builder("ts-cli");
  RETURN_NOT_OK(builder.Build(&messenger_));

  RETURN_NOT_OK(host_port.ResolveAddresses(&addrs_))

  generic_proxy_.reset(new server::GenericServiceProxy(messenger_, addrs_[0]));
  ts_proxy_.reset(new TabletServerServiceProxy(messenger_, addrs_[0]));
  ts_admin_proxy_.reset(new TabletServerAdminServiceProxy(messenger_, addrs_[0]));

  initted_ = true;

  VLOG(1) << "Connected to " << addr_;

  return Status::OK();
}

Status TsAdminClient::ListTablets(vector<StatusAndSchemaPB>* tablets) {
  CHECK(initted_);

  ListTabletsRequestPB req;
  ListTabletsResponsePB resp;
  RpcController rpc;

  rpc.set_timeout(timeout_);
  RETURN_NOT_OK(ts_proxy_->ListTablets(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  tablets->assign(resp.status_and_schema().begin(), resp.status_and_schema().end());

  return Status::OK();
}

Status TsAdminClient::SetFlag(const string& flag, const string& val,
                              bool force) {
  server::SetFlagRequestPB req;
  server::SetFlagResponsePB resp;
  RpcController rpc;

  rpc.set_timeout(timeout_);
  req.set_flag(flag);
  req.set_value(val);
  req.set_force(force);

  RETURN_NOT_OK(generic_proxy_->SetFlag(req, &resp, &rpc));
  switch (resp.result()) {
    case server::SetFlagResponsePB::SUCCESS:
      return Status::OK();
    case server::SetFlagResponsePB::NOT_SAFE:
      return Status::RemoteError(resp.msg() + " (use --force flag to allow anyway)");
    default:
      return Status::RemoteError(resp.ShortDebugString());
  }
}

Status TsAdminClient::GetTabletSchema(const std::string& tablet_id,
                                      SchemaPB* schema) {
  VLOG(1) << "Fetching schema for tablet " << tablet_id;
  vector<StatusAndSchemaPB> tablets;
  RETURN_NOT_OK(ListTablets(&tablets));
  BOOST_FOREACH(const StatusAndSchemaPB& pair, tablets) {
    if (pair.tablet_status().tablet_id() == tablet_id) {
      *schema = pair.schema();
      return Status::OK();
    }
  }
  return Status::NotFound("Cannot find tablet", tablet_id);
}

Status TsAdminClient::DumpTablet(const std::string& tablet_id) {
  SchemaPB schema_pb;
  RETURN_NOT_OK(GetTabletSchema(tablet_id, &schema_pb));
  Schema schema;
  RETURN_NOT_OK(SchemaFromPB(schema_pb, &schema));

  ScanRequestPB req;
  ScanResponsePB resp;

  NewScanRequestPB* new_req = req.mutable_new_scan_request();
  RETURN_NOT_OK(SchemaToColumnPBs(
      schema, new_req->mutable_projected_columns(),
      SCHEMA_PB_WITHOUT_IDS | SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES));
  new_req->set_tablet_id(tablet_id);
  new_req->set_cache_blocks(false);
  new_req->set_order_mode(ORDERED);
  new_req->set_read_mode(READ_AT_SNAPSHOT);

  vector<KuduRowResult> rows;
  while (true) {
    RpcController rpc;
    rpc.set_timeout(timeout_);
    RETURN_NOT_OK_PREPEND(ts_proxy_->Scan(req, &resp, &rpc),
                          "Scan() failed");

    if (resp.has_error()) {
      return Status::IOError("Failed to read: ", resp.error().ShortDebugString());
    }

    rows.clear();
    RETURN_NOT_OK(KuduScanner::Data::ExtractRows(rpc, &schema, &resp, &rows));
    BOOST_FOREACH(const KuduRowResult& r, rows) {
      std::cout << r.ToString() << std::endl;
    }

    // The first response has a scanner ID. We use this for all subsequent
    // responses.
    if (resp.has_scanner_id()) {
      req.set_scanner_id(resp.scanner_id());
      req.clear_new_scan_request();
    }
    req.set_call_seq_id(req.call_seq_id() + 1);
    if (!resp.has_more_results()) {
      break;
    }
  }
  return Status::OK();
}

Status TsAdminClient::DeleteTablet(const std::string& tablet_id,
                                   const std::string& reason) {
  DeleteTabletRequestPB req;
  DeleteTabletResponsePB resp;
  RpcController rpc;

  req.set_tablet_id(tablet_id);
  req.set_reason(reason);
  req.set_delete_type(tablet::TABLET_DATA_TOMBSTONED);
  rpc.set_timeout(timeout_);
  RETURN_NOT_OK_PREPEND(ts_admin_proxy_->DeleteTablet(req, &resp, &rpc),
                        "DeleteTablet() failed");

  if (resp.has_error()) {
    return Status::IOError("Failed to delete tablet: ",
                           resp.error().ShortDebugString());
  }
  return Status::OK();
}

Status TsAdminClient::CurrentTimestamp(uint64_t* timestamp) {
  server::ServerClockRequestPB req;
  server::ServerClockResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout_);
  RETURN_NOT_OK(generic_proxy_->ServerClock(req, &resp, &rpc));
  CHECK(resp.has_timestamp()) << resp.DebugString();
  *timestamp = resp.timestamp();
  return Status::OK();
}

Status TsAdminClient::GetStatus(ServerStatusPB* pb) {
  server::GetStatusRequestPB req;
  server::GetStatusResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(timeout_);
  RETURN_NOT_OK(generic_proxy_->GetStatus(req, &resp, &rpc));
  CHECK(resp.has_status()) << resp.DebugString();
  pb->Swap(resp.mutable_status());
  return Status::OK();
}

namespace {

void SetUsage(const char* argv0) {
  ostringstream str;

  str << argv0 << " [-tserver_address=<addr>] <operation> <flags>\n"
      << "<operation> must be one of:\n"
      << "  " << kListTabletsOp << "\n"
      << "  " << kAreTabletsRunningOp << "\n"
      << "  " << kSetFlagOp << " [-force] <flag> <value>\n"
      << "  " << kDumpTabletOp << " <tablet_id>\n"
      << "  " << kDeleteTabletOp << " <tablet_id> <reason string>\n"
      << "  " << kCurrentTimestamp << "\n"
      << "  " << kStatus;
  google::SetUsageMessage(str.str());
}

string GetOp(int argc, char** argv) {
  if (argc < 2) {
    google::ShowUsageWithFlagsRestrict(argv[0], __FILE__);
    exit(1);
  }

  return argv[1];
}

} // anonymous namespace

static int TsCliMain(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  SetUsage(argv[0]);
  ParseCommandLineFlags(&argc, &argv, true);
  InitGoogleLoggingSafe(argv[0]);
  const string addr = FLAGS_server_address;

  string op = GetOp(argc, argv);

  TsAdminClient client(addr, FLAGS_timeout_ms);

  RETURN_NOT_OK_PREPEND_FROM_MAIN(client.Init(),
                                  "Unable to establish connection to " + addr);

  // TODO add other operations here...
  if (op == kListTabletsOp) {
    CHECK_ARGC_OR_RETURN_WITH_USAGE(2);

    vector<StatusAndSchemaPB> tablets;
    RETURN_NOT_OK_PREPEND_FROM_MAIN(client.ListTablets(&tablets),
                                    "Unable to list tablets on " + addr);
    BOOST_FOREACH(const StatusAndSchemaPB& status_and_schema, tablets) {
      Schema schema;
      RETURN_NOT_OK_PREPEND_FROM_MAIN(SchemaFromPB(status_and_schema.schema(), &schema),
                                      "Unable to deserialize schema from " + addr);
      PartitionSchema partition_schema;
      RETURN_NOT_OK_PREPEND_FROM_MAIN(PartitionSchema::FromPB(status_and_schema.partition_schema(),
                                                              schema, &partition_schema),
                                      "Unable to deserialize partition schema from " + addr);


      TabletStatusPB ts = status_and_schema.tablet_status();

      Partition partition;
      Partition::FromPB(ts.partition(), &partition);

      string state = tablet::TabletStatePB_Name(ts.state());
      std::cout << "Tablet id: " << ts.tablet_id() << std::endl;
      std::cout << "State: " << state << std::endl;
      std::cout << "Table name: " << ts.table_name() << std::endl;
      std::cout << "Partition: " << partition_schema.PartitionDebugString(partition, schema)
                << std::endl;
      if (ts.has_estimated_on_disk_size()) {
        std::cout << "Estimated on disk size: " <<
            HumanReadableNumBytes::ToString(ts.estimated_on_disk_size()) << std::endl;
      }
      std::cout << "Schema: " << schema.ToString() << std::endl;
    }
  } else if (op == kAreTabletsRunningOp) {
    CHECK_ARGC_OR_RETURN_WITH_USAGE(2);

    vector<StatusAndSchemaPB> tablets;
    RETURN_NOT_OK_PREPEND_FROM_MAIN(client.ListTablets(&tablets),
                                    "Unable to list tablets on " + addr);
    bool all_running = true;
    BOOST_FOREACH(const StatusAndSchemaPB& status_and_schema, tablets) {
      TabletStatusPB ts = status_and_schema.tablet_status();
      if (ts.state() != tablet::RUNNING) {
        std::cout << "Tablet id: " << ts.tablet_id() << " is "
                  << tablet::TabletStatePB_Name(ts.state()) << std::endl;
        all_running = false;
      }
    }

    if (all_running) {
      std::cout << "All tablets are running" << std::endl;
    } else {
      std::cout << "Not all tablets are running" << std::endl;
      return 1;
    }
  } else if (op == kSetFlagOp) {
    CHECK_ARGC_OR_RETURN_WITH_USAGE(4);

    RETURN_NOT_OK_PREPEND_FROM_MAIN(client.SetFlag(argv[2], argv[3], FLAGS_force),
                                    "Unable to set flag");

  } else if (op == kDumpTabletOp) {
    CHECK_ARGC_OR_RETURN_WITH_USAGE(3);

    string tablet_id = argv[2];
    RETURN_NOT_OK_PREPEND_FROM_MAIN(client.DumpTablet(tablet_id),
                                    "Unable to dump tablet");
  } else if (op == kDeleteTabletOp) {
    CHECK_ARGC_OR_RETURN_WITH_USAGE(4);

    string tablet_id = argv[2];
    string reason = argv[3];

    RETURN_NOT_OK_PREPEND_FROM_MAIN(client.DeleteTablet(tablet_id, reason),
                                    "Unable to delete tablet");
  } else if (op == kCurrentTimestamp) {
    CHECK_ARGC_OR_RETURN_WITH_USAGE(2);

    uint64_t timestamp;
    RETURN_NOT_OK_PREPEND_FROM_MAIN(client.CurrentTimestamp(&timestamp),
                                    "Unable to get timestamp");
    std::cout << timestamp << std::endl;
  } else if (op == kStatus) {
    CHECK_ARGC_OR_RETURN_WITH_USAGE(2);

    ServerStatusPB status;
    RETURN_NOT_OK_PREPEND_FROM_MAIN(client.GetStatus(&status),
                                    "Unable to get status");
    std::cout << status.DebugString() << std::endl;
  } else {
    std::cerr << "Invalid operation: " << op << std::endl;
    google::ShowUsageWithFlagsRestrict(argv[0], __FILE__);
    return 2;
  }

  return 0;
}

} // namespace tools
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::tools::TsCliMain(argc, argv);
}
