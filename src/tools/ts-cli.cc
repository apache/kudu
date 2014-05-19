// Copyright (c) 2014, Cloudera, inc.
//
// Tool to query tablet server operational data

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <tr1/memory>
#include <iostream>

#include "common/wire_protocol.h"
#include "tserver/tserver.pb.h"
#include "tserver/tserver_service.proxy.h"
#include "tserver/tablet_server.h"
#include "util/env.h"
#include "util/faststring.h"
#include "util/logging.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "rpc/messenger.h"
#include "rpc/rpc_controller.h"

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using kudu::tablet::TabletStatusPB;
using kudu::tserver::TabletServerServiceProxy;
using kudu::tserver::ListTabletsRequestPB;
using kudu::tserver::ListTabletsResponsePB;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::HostPort;
using kudu::Sockaddr;

DEFINE_string(tserver_address, "localhost",
                "Address of tablet server to run against");
DEFINE_string(op, "list_tablets", "Operation to execute");
DEFINE_int64(timeout_ms, 1000 * 60, "RPC timeout in milliseconds");

namespace {

// TODO once more operations are supported, print a more useful error
// message
bool ValidateOp(const char* flagname, const string& op) {
  if (op == "list_tablets") {
    return true;
  }
  std::cerr << "Invalid operation " << op << ", valid operations are: "
            << "list_tablets" << std::endl;
  return false;
}

const bool op_dummy = google::RegisterFlagValidator(&FLAGS_op, &ValidateOp);

} // anonymous namespace

namespace kudu {
namespace tools {

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
  Status ListTablets(std::vector<tablet::TabletStatusPB>* tablets);

 private:
  std::string addr_;
  MonoDelta timeout_;
  bool initted_;
  gscoped_ptr<tserver::TabletServerServiceProxy> proxy_;
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
  vector<Sockaddr> addrs;

  RETURN_NOT_OK(host_port.ResolveAddresses(&addrs))

  proxy_.reset(new TabletServerServiceProxy(messenger_, addrs[0]));

  initted_ = true;

  LOG(INFO) << "Connected to " << addr_;

  return Status::OK();
}

Status TsAdminClient::ListTablets(vector<TabletStatusPB>* tablets) {
  CHECK(initted_);

  ListTabletsRequestPB req;
  ListTabletsResponsePB resp;
  RpcController rpc;

  rpc.set_timeout(timeout_);
  RETURN_NOT_OK(proxy_->ListTablets(req, &resp, &rpc));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  tablets->assign(resp.tablet_status().begin(), resp.tablet_status().end());

  return Status::OK();
}

static int TsCliMain(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  google::ParseCommandLineFlags(&argc, &argv, true);
  InitGoogleLoggingSafe(argv[0]);
  const string addr = FLAGS_tserver_address;

  TsAdminClient client(addr, FLAGS_timeout_ms);

  CHECK_OK_PREPEND(client.Init(), "Unable to establish connection to " + addr);

  // TODO add other operations here...
  if (FLAGS_op == "list_tablets") {
    vector<TabletStatusPB> tablets;
    CHECK_OK_PREPEND(client.ListTablets(&tablets), "Unable to list tablets on " + addr);
    BOOST_FOREACH(const TabletStatusPB& tablet, tablets) {
      std::cout << tablet.DebugString() << std::endl;
    }
  } else {
    LOG(FATAL) << "Invalid op specified: " << FLAGS_op;
  }

  return 0;
}

} // namespace tools
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::tools::TsCliMain(argc, argv);
}
