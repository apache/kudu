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

#include "kudu/tools/tool_action.h"

#include <unistd.h>

#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/status.h>
#include <google/protobuf/stubs/stringpiece.h>
#include <google/protobuf/util/json_util.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/tools/tool.pb.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

DEFINE_string(serialization, "json", "Serialization method to be used by the "
              "control shell. Valid values are 'json' (protobuf serialized "
              "into JSON and terminated with a newline character) or 'pb' "
              "(four byte protobuf message length in big endian followed by "
              "the protobuf message itself).");
DEFINE_validator(serialization, [](const char* /*n*/, const std::string& v) {
  return boost::iequals(v, "pb") ||
         boost::iequals(v, "json");
});

namespace kudu {

namespace tools {

using cluster::ExternalDaemon;
using cluster::ExternalMiniCluster;
using cluster::ExternalMiniClusterOptions;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace {

Status MakeClusterRoot(string* cluster_root) {
  // The ExternalMiniCluster can't generate the cluster root on our behalf because
  // we're not running inside a gtest. So we'll use this approach instead,
  // which is what the Java external mini cluster used for a long time.
  const char* tmpdir = getenv("TEST_TMPDIR");
  string tmpdir_str = tmpdir ? tmpdir : Substitute("/tmp/kudutest-$0", getuid());
  string root = JoinPathSegments(tmpdir_str, "minicluster-data");
  RETURN_NOT_OK(env_util::CreateDirsRecursively(Env::Default(), root));

  *cluster_root = root;
  return Status::OK();
}

Status CheckClusterExists(const unique_ptr<ExternalMiniCluster>& cluster) {
  if (!cluster) {
    return Status::NotFound("cluster not found");
  }
  return Status::OK();
}

Status FindDaemon(const unique_ptr<ExternalMiniCluster>& cluster,
                  const DaemonIdentifierPB& id,
                  ExternalDaemon** daemon,
                  MiniKdc** kdc) {
  RETURN_NOT_OK(CheckClusterExists(cluster));

  if (!id.has_type()) {
    return Status::InvalidArgument("request is missing daemon type");
  }

  switch (id.type()) {
    case MASTER:
      if (!id.has_index()) {
        return Status::InvalidArgument("request is missing daemon index");
      }
      if (id.index() >= cluster->num_masters()) {
        return Status::NotFound(Substitute("no master with index $0",
                                           id.index()));
      }
      *daemon = cluster->master(id.index());
      *kdc = nullptr;
      break;
    case TSERVER:
      if (!id.has_index()) {
        return Status::InvalidArgument("request is missing daemon index");
      }
      if (id.index() >= cluster->num_tablet_servers()) {
        return Status::NotFound(Substitute("no tserver with index $0",
                                           id.index()));
      }
      *daemon = cluster->tablet_server(id.index());
      *kdc = nullptr;
      break;
    case KDC:
      if (!cluster->kdc()) {
        return Status::NotFound("kdc not found");
      }
      *daemon = nullptr;
      *kdc = cluster->kdc();
      break;
    default:
      return Status::InvalidArgument(
          Substitute("unknown daemon type: $0", DaemonType_Name(id.type())));
  }
  return Status::OK();
}

Status ProcessRequest(const ControlShellRequestPB& req,
                      ControlShellResponsePB* resp,
                      unique_ptr<ExternalMiniCluster>* cluster) {
  switch (req.request_case()) {
    case ControlShellRequestPB::kCreateCluster:
    {
      if (*cluster) {
        RETURN_NOT_OK(Status::InvalidArgument("cluster already created"));
      }
      const CreateClusterRequestPB& cc = req.create_cluster();
      ExternalMiniClusterOptions opts;
      if (cc.has_num_masters()) {
        if (cc.num_masters() != 1 && cc.num_masters() != 3) {
          RETURN_NOT_OK(Status::InvalidArgument(
              "only one or three masters are supported"));
        }
        opts.num_masters = cc.num_masters();
      }
      if (cc.has_num_tservers()) {
        opts.num_tablet_servers = cc.num_tservers();
      }
      opts.enable_kerberos = cc.enable_kerberos();
      opts.hms_mode = cc.hms_mode();
      if (cc.has_cluster_root()) {
        opts.cluster_root = cc.cluster_root();
      } else {
        RETURN_NOT_OK(MakeClusterRoot(&opts.cluster_root));
      }
      opts.extra_master_flags.assign(cc.extra_master_flags().begin(),
                                     cc.extra_master_flags().end());
      opts.extra_tserver_flags.assign(cc.extra_tserver_flags().begin(),
                                      cc.extra_tserver_flags().end());
      if (opts.enable_kerberos) {
        opts.mini_kdc_options.data_root = JoinPathSegments(opts.cluster_root, "krb5kdc");
        opts.mini_kdc_options.ticket_lifetime = cc.mini_kdc_options().ticket_lifetime();
        opts.mini_kdc_options.renew_lifetime = cc.mini_kdc_options().renew_lifetime();
      }

      cluster->reset(new ExternalMiniCluster(std::move(opts)));
      break;
    }
    case ControlShellRequestPB::kDestroyCluster:
    {
      RETURN_NOT_OK(CheckClusterExists(*cluster));
      cluster->reset();
      break;
    }
    case ControlShellRequestPB::kStartCluster:
    {
      RETURN_NOT_OK(CheckClusterExists(*cluster));
      if ((*cluster)->num_masters() != 0) {
        DCHECK_GT((*cluster)->num_tablet_servers(), 0);
        RETURN_NOT_OK((*cluster)->Restart());
      } else {
        RETURN_NOT_OK((*cluster)->Start());
      }
      break;
    }
    case ControlShellRequestPB::kStopCluster:
    {
      RETURN_NOT_OK(CheckClusterExists(*cluster));
      (*cluster)->Shutdown();
      break;
    }
    case ControlShellRequestPB::kStartDaemon:
    {
      if (!req.start_daemon().has_id()) {
        RETURN_NOT_OK(Status::InvalidArgument("missing process id"));
      }
      ExternalDaemon* daemon;
      MiniKdc* kdc;
      RETURN_NOT_OK(FindDaemon(*cluster, req.start_daemon().id(), &daemon, &kdc));
      if (daemon) {
        DCHECK(!kdc);
        RETURN_NOT_OK(daemon->Restart());
      } else {
        DCHECK(kdc);
        RETURN_NOT_OK(kdc->Start());
      }
      break;
    }
    case ControlShellRequestPB::kStopDaemon:
    {
      if (!req.stop_daemon().has_id()) {
        RETURN_NOT_OK(Status::InvalidArgument("missing process id"));
      }
      ExternalDaemon* daemon;
      MiniKdc* kdc;
      RETURN_NOT_OK(FindDaemon(*cluster, req.stop_daemon().id(), &daemon, &kdc));
      if (daemon) {
        DCHECK(!kdc);
        daemon->Shutdown();
      } else {
        DCHECK(kdc);
        RETURN_NOT_OK(kdc->Stop());
      }
      break;
    }
    case ControlShellRequestPB::kGetMasters:
    {
      RETURN_NOT_OK(CheckClusterExists(*cluster));
      for (int i = 0; i < (*cluster)->num_masters(); i++) {
        HostPortPB pb;
        RETURN_NOT_OK(HostPortToPB((*cluster)->master(i)->bound_rpc_hostport(), &pb));
        DaemonInfoPB* info = resp->mutable_get_masters()->mutable_masters()->Add();
        info->mutable_id()->set_type(MASTER);
        info->mutable_id()->set_index(i);
        *info->mutable_bound_rpc_address() = std::move(pb);
      }
      break;
    }
    case ControlShellRequestPB::kGetTservers:
    {
      RETURN_NOT_OK(CheckClusterExists(*cluster));
      for (int i = 0; i < (*cluster)->num_tablet_servers(); i++) {
        HostPortPB pb;
        RETURN_NOT_OK(HostPortToPB((*cluster)->tablet_server(i)->bound_rpc_hostport(), &pb));
        DaemonInfoPB* info = resp->mutable_get_tservers()->mutable_tservers()->Add();
        info->mutable_id()->set_type(TSERVER);
        info->mutable_id()->set_index(i);
        *info->mutable_bound_rpc_address() = std::move(pb);
      }
      break;
    }
    case ControlShellRequestPB::kGetKdcEnvVars:
    {
      if (!(*cluster)->kdc()) {
        RETURN_NOT_OK(Status::NotFound("kdc not found"));
      }
      auto env_vars = (*cluster)->kdc()->GetEnvVars();
      resp->mutable_get_kdc_env_vars()->mutable_env_vars()->insert(
          env_vars.begin(), env_vars.end());
      break;
    }
    case ControlShellRequestPB::kKdestroy:
    {
      if (!(*cluster)->kdc()) {
        RETURN_NOT_OK(Status::NotFound("kdc not found"));
      }
      RETURN_NOT_OK((*cluster)->kdc()->Kdestroy());
      break;
    }
    case ControlShellRequestPB::kKinit:
    {
      if (!(*cluster)->kdc()) {
        RETURN_NOT_OK(Status::NotFound("kdc not found"));
      }
      RETURN_NOT_OK((*cluster)->kdc()->Kinit(req.kinit().username()));
      break;
    }
    case ControlShellRequestPB::kSetDaemonFlag:
    {
      const auto& r = req.set_daemon_flag();
      if (!r.has_id()) {
        RETURN_NOT_OK(Status::InvalidArgument("missing process id"));
      }
      const auto& id = r.id();
      if (id.type() == DaemonType::KDC) {
        return Status::InvalidArgument("mini-KDC doesn't support SetFlag()");
      }
      ExternalDaemon* daemon;
      MiniKdc* kdc;
      RETURN_NOT_OK(FindDaemon(*cluster, id, &daemon, &kdc));
      DCHECK(daemon);
      RETURN_NOT_OK((*cluster)->SetFlag(daemon, r.flag(), r.value()));
      break;
    }
    default:
      RETURN_NOT_OK(Status::InvalidArgument("unknown cluster control request"));
  }

  return Status::OK();
}

Status RunControlShell(const RunnerContext& /*context*/) {
  // Set up the protocol.
  //
  // Because we use stdin and stdout to communicate with the shell's parent,
  // it's critical that none of our subprocesses write to stdout. To that end,
  // the protocol will use stdout via another fd, and we'll redirect fd 1 to stderr.
  int new_stdout;
  RETRY_ON_EINTR(new_stdout, dup(STDOUT_FILENO));
  CHECK_ERR(new_stdout);
  int ret;
  RETRY_ON_EINTR(ret, dup2(STDERR_FILENO, STDOUT_FILENO));
  PCHECK(ret == STDOUT_FILENO);
  ControlShellProtocol::SerializationMode serde_mode;
  if (boost::iequals(FLAGS_serialization, "json")) {
    serde_mode = ControlShellProtocol::SerializationMode::JSON;
  } else {
    DCHECK(boost::iequals(FLAGS_serialization, "pb"));
    serde_mode = ControlShellProtocol::SerializationMode::PB;
  }
  ControlShellProtocol protocol(serde_mode,
                                ControlShellProtocol::CloseMode::NO_CLOSE_ON_DESTROY,
                                STDIN_FILENO,
                                new_stdout);

  // Run the shell loop, processing each message as it is received.
  unique_ptr<ExternalMiniCluster> cluster;
  while (true) {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;

    // Receive a new request, blocking until one is received.
    //
    // IO errors are fatal while others will result in an error response.
    Status s = protocol.ReceiveMessage(&req);
    if (s.IsEndOfFile()) {
      break;
    }
    if (s.IsIOError()) {
      return s;
    }

    // If we've made it here, we're definitely going to respond.

    if (s.ok()) {
      // We've successfully received a message. Try to process it.
      s = ProcessRequest(req, &resp, &cluster);
    }

    if (!s.ok()) {
      // This may be the result of ReceiveMessage() or ProcessRequest(),
      // whichever failed first.
      StatusToPB(s, resp.mutable_error());
    }

    // Send the response. All errors are fatal.
    s = protocol.SendMessage(resp);
    if (s.IsEndOfFile()) {
      break;
    }
    RETURN_NOT_OK(s);
  }

  // Normal exit, clean up cluster root.
  if (cluster) {
    cluster->Shutdown();
    WARN_NOT_OK(Env::Default()->DeleteRecursively(cluster->cluster_root()),
                "Could not delete cluster root");
  }
  return Status::OK();
}

string SerializeRequest(const ControlShellRequestPB& req) {
  string serialized;
  auto google_status = google::protobuf::util::MessageToJsonString(
      req, &serialized);
  CHECK(google_status.ok()) << Substitute(
      "unable to serialize JSON ($0): $1",
      google_status.error_message().ToString(), pb_util::SecureDebugString(req));
  return serialized;
}

} // anonymous namespace

unique_ptr<Mode> BuildTestMode() {

  ControlShellRequestPB create;
  create.mutable_create_cluster()->set_num_tservers(3);
  ControlShellRequestPB start;
  start.mutable_start_cluster();

  string extra = Substitute(
      "The protocol for the control shell is protobuf-based and is documented "
      "in src/kudu/tools/tool.proto. It is currently considered to be highly "
      "experimental and subject to change.\n"
      "\n"
      "Example JSON input to create and start a cluster:\n"
      "    $0\n"
      "    $1\n",
      SerializeRequest(create),
      SerializeRequest(start));

  unique_ptr<Action> control_shell =
      ActionBuilder("mini_cluster", &RunControlShell)
      .Description("Spawn a control shell for running a mini-cluster")
      .ExtraDescription(extra)
      .AddOptionalParameter("serialization")
      .Build();

  return ModeBuilder("test")
      .Description("Various test actions")
      .AddAction(std::move(control_shell))
      .Build();
}

} // namespace tools
} // namespace kudu

