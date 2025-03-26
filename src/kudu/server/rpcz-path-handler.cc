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

#include "kudu/server/rpcz-path-handler.h"

#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/rpc/messenger.h" // IWYU pragma: keep
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/rpcz_store.h"
#include "kudu/server/webserver.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/status.h"
#include "kudu/util/web_callback_registry.h"

using kudu::rpc::DumpConnectionsRequestPB;
using kudu::rpc::DumpConnectionsResponsePB;
using kudu::rpc::DumpRpczStoreRequestPB;
using kudu::rpc::DumpRpczStoreResponsePB;
using kudu::rpc::Messenger;
using std::ostringstream;
using std::shared_ptr;
using std::string;

namespace kudu {

namespace {

void RpczPathHandler(const shared_ptr<Messenger>& messenger,
                     const Webserver::WebRequest& req,
                     Webserver::PrerenderedWebResponse* resp) {
  JsonWriter writer(&resp->output, JsonWriter::PRETTY);
  writer.StartObject();
  {
    static const string kParamName = "include_traces";
    static const string kParamVal = "false";
    const string& arg = FindWithDefault(req.parsed_args, kParamName, kParamVal);
    DumpConnectionsRequestPB dump_req;
    dump_req.set_include_traces(ParseLeadingBoolValue(arg.c_str(), false));

    DumpConnectionsResponsePB running_rpcs;
    const auto& s = messenger->DumpConnections(dump_req, &running_rpcs);
    if (s.ok()) {
      writer.String("running");
      writer.Protobuf(running_rpcs);
    } else {
      WARN_NOT_OK(s, "error dumping info on RPC connections");
    }
  }
  {
    DumpRpczStoreResponsePB sampled_rpcs;
    messenger->rpcz_store()->DumpPB({}, &sampled_rpcs);
    writer.String("sampled");
    writer.Protobuf(sampled_rpcs);
  }
  writer.EndObject();
}

} // anonymous namespace

void AddRpczPathHandlers(const shared_ptr<Messenger>& messenger, Webserver* webserver) {
  webserver->RegisterJsonPathHandler(
      "/rpcz", "RPCs",
      [messenger](const Webserver::WebRequest& req, Webserver::PrerenderedWebResponse* resp) {
        RpczPathHandler(messenger, req, resp);
      },
      true /*is_on_nav_bar*/);
}

} // namespace kudu
