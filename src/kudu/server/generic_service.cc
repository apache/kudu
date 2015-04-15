// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/server/generic_service.h"

#include <gflags/gflags.h>
#include <string>
#include <tr1/unordered_set>

#include "kudu/gutil/map-util.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/server/server_base.h"
#include "kudu/util/flag_tags.h"

using std::string;
using std::tr1::unordered_set;

namespace kudu {
namespace server {

GenericServiceImpl::GenericServiceImpl(ServerBase* server)
  : GenericServiceIf(server->metric_entity()),
    server_(server) {
}

GenericServiceImpl::~GenericServiceImpl() {
}

void GenericServiceImpl::SetFlag(const SetFlagRequestPB* req,
                                 SetFlagResponsePB* resp,
                                 rpc::RpcContext* rpc) {

  // Validate that the flag exists and get the current value.
  string old_val;
  if (!google::GetCommandLineOption(req->flag().c_str(),
                                    &old_val)) {
    resp->set_result(SetFlagResponsePB::NO_SUCH_FLAG);
    rpc->RespondSuccess();
    return;
  }

  // Validate that the flag is runtime-changeable.
  unordered_set<string> tags;
  GetFlagTags(req->flag(), &tags);
  if (!ContainsKey(tags, "runtime")) {
    if (req->force()) {
      LOG(WARNING) << rpc->requestor_string() << " forcing change of "
                   << "non-runtime-safe flag " << req->flag();
    } else {
      resp->set_result(SetFlagResponsePB::NOT_SAFE);
      resp->set_msg("Flag is not safe to change at runtime");
      rpc->RespondSuccess();
      return;
    }
  }

  resp->set_old_value(old_val);

  // Try to set the new value.
  string ret = google::SetCommandLineOption(
      req->flag().c_str(),
      req->value().c_str());
  if (ret.empty()) {
    resp->set_result(SetFlagResponsePB::BAD_VALUE);
    resp->set_msg("Unable to set flag: bad value");
  } else {
    LOG(INFO) << rpc->requestor_string() << " changed flags via RPC: "
              << req->flag() << " from '" << old_val << "' to '"
              << req->value() << "'";
    resp->set_result(SetFlagResponsePB::SUCCESS);
    resp->set_msg(ret);
  }

  rpc->RespondSuccess();
}

} // namespace server
} // namespace kudu
