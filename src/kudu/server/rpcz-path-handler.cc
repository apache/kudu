// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/server/rpcz-path-handler.h"

#include <boost/bind.hpp>
#include <tr1/memory>
#include <fstream>
#include <string>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/server/webserver.h"

using kudu::rpc::Messenger;
using kudu::rpc::DumpRunningRpcsRequestPB;
using kudu::rpc::DumpRunningRpcsResponsePB;
using std::stringstream;

namespace kudu {

namespace {

void RpczPathHandler(const std::tr1::shared_ptr<Messenger>& messenger,
                     const Webserver::WebRequest& req, stringstream* output) {
  DumpRunningRpcsRequestPB dump_req;
  DumpRunningRpcsResponsePB dump_resp;

  string arg = FindWithDefault(req.parsed_args, "include_traces", "false");
  dump_req.set_include_traces(ParseLeadingBoolValue(arg.c_str(), false));

  messenger->DumpRunningRpcs(dump_req, &dump_resp);

  JsonWriter writer(output, JsonWriter::PRETTY);
  writer.Protobuf(dump_resp);
}

} // anonymous namespace

void AddRpczPathHandlers(const std::tr1::shared_ptr<Messenger>& messenger, Webserver* webserver) {
  webserver->RegisterPathHandler("/rpcz", "RPCs",
                                 boost::bind(RpczPathHandler, messenger, _1, _2),
                                 false, true);
}

} // namespace kudu
