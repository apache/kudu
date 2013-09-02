// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "rpc/negotiation.h"

#include <string>
#include <boost/lexical_cast.hpp>

#include <glog/logging.h>

#include "rpc/blocking_ops.h"
#include "rpc/connection.h"
#include "rpc/reactor.h"
#include "rpc/sasl_client.h"
#include "rpc/sasl_common.h"
#include "rpc/sasl_server.h"
#include "util/task_executor.h"
#include "util/status.h"

namespace kudu {
namespace rpc {

using std::tr1::shared_ptr;

///
/// ClientNegotiationTask
///

ClientNegotiationTask::ClientNegotiationTask(const shared_ptr<Connection>& conn)
  : conn_(conn) {
}

Status ClientNegotiationTask::Run() {
  Status s = conn_->sasl_client().Negotiate();
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << "Client SASL negotiation failed: " << s.ToString();
    return s;
  }
  return Status::OK();
}

bool ClientNegotiationTask::Abort() {
  return false;
}

///
/// ServerNegotiationTask
///

ServerNegotiationTask::ServerNegotiationTask(const shared_ptr<Connection>& conn)
  : conn_(conn) {
}

Status ServerNegotiationTask::Run() {
  Status s = conn_->sasl_server().Negotiate();
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << "Server SASL negotiation failed: " << s.ToString();
    return s;
  }
  return Status::OK();
}

bool ServerNegotiationTask::Abort() {
  return false;
}

///
/// NegotiationCallback
///

NegotiationCallback::NegotiationCallback(const shared_ptr<Connection>& conn)
  : conn_(conn) {
}

void NegotiationCallback::OnSuccess() {
  conn_->CompleteNegotiation(Status::OK());
}

void NegotiationCallback::OnFailure(const Status& status) {
  conn_->CompleteNegotiation(status);
}

} // namespace rpc
} // namespace kudu
