// Copyright (c) 2014, Cloudera, inc.

#include "server/rpcz-path-handler.h"

#include <tr1/memory>
#include <fstream>
#include <string>

#include "gutil/map-util.h"
#include "gutil/strings/numbers.h"
#include "rpc/messenger.h"
#include "rpc/rpc_introspection.pb.h"
#include "server/webserver.h"

using kudu::rpc::Messenger;
using kudu::rpc::DumpRunningRpcsRequestPB;
using kudu::rpc::DumpRunningRpcsResponsePB;
using std::stringstream;

namespace kudu {

namespace {

void RpczPathHandler(const std::tr1::shared_ptr<Messenger>& messenger,
                     const Webserver::ArgumentMap& args, stringstream* output) {
  DumpRunningRpcsRequestPB dump_req;
  DumpRunningRpcsResponsePB dump_resp;

  string arg = FindWithDefault(args, "include_traces", "false");
  dump_req.set_include_traces(ParseLeadingBoolValue(arg.c_str(), false));

  messenger->DumpRunningRpcs(dump_req, &dump_resp);

  JsonWriter writer(output);
  writer.Protobuf(dump_resp);
}

} // anonymous namespace

void AddRpczPathHandlers(const std::tr1::shared_ptr<Messenger>& messenger, Webserver* webserver) {
  webserver->RegisterPathHandler("/rpcz",
                                 boost::bind(RpczPathHandler, messenger, _1, _2),
                                 false, true);
}

} // namespace kudu
