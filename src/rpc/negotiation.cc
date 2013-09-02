// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "rpc/negotiation.h"

#include <string>
#include <boost/lexical_cast.hpp>

#include <glog/logging.h>

#include "rpc/blocking_ops.h"
#include "rpc/connection.h"
#include "rpc/reactor.h"
#include "rpc/rpc_header.pb.h"
#include "rpc/sasl_client.h"
#include "rpc/sasl_common.h"
#include "rpc/sasl_server.h"
#include "util/task_executor.h"
#include "util/status.h"

namespace kudu {
namespace rpc {

using std::tr1::shared_ptr;

// Client: Send ConnectionContextPB message based on information stored in the Connection object.
static Status SendConnectionContext(Connection* conn) {
  RequestHeader header;
  header.set_callid(kConnectionContextCallId);

  ConnectionContextPB conn_context;
  conn_context.set_servicename(conn->service_name());
  conn_context.mutable_userinfo()->set_effectiveuser(conn->user_cred().effective_user());
  conn_context.mutable_userinfo()->set_realuser(conn->user_cred().real_user());

  return SendFramedMessageBlocking(conn->socket(), header, conn_context);
}

// Server: Receive ConnectionContextPB message and update the corresponding fields in the
// associated Connection object. Perform validation against SASL-negotiated information
// as needed.
static Status RecvConnectionContext(Connection* conn) {
  faststring recv_buf(1024); // Should be plenty for a ConnectionContextPB message.
  RequestHeader header;
  Slice param_buf;
  RETURN_NOT_OK(ReceiveFramedMessageBlocking(conn->socket(), &recv_buf, &header, &param_buf));
  DCHECK(header.IsInitialized());

  if (header.callid() != kConnectionContextCallId) {
    return Status::IllegalState("Expected ConnectionContext callid, received",
        boost::lexical_cast<string>(header.callid()));
  }

  ConnectionContextPB conn_context;
  if (!conn_context.ParseFromArray(param_buf.data(), param_buf.size())) {
    return Status::InvalidArgument("Invalid ConnectionContextPB message, missing fields",
        conn_context.InitializationErrorString());
  }

  // Update the fields of our Connection object from the ConnectionContextPB.
  conn->set_service_name(conn_context.servicename());
  if (conn_context.has_userinfo()) {
    // Validate real user against SASL impl.
    if (conn->sasl_server().negotiated_mechanism() == SaslMechanism::PLAIN) {
      if (conn->sasl_server().plain_auth_user() != conn_context.userinfo().realuser()) {
        return Status::NotAuthorized(
            "ConnectionContextPB specified different real user than sent in SASL negotiation",
            StringPrintf("\"%s\" vs. \"%s\"",
                conn_context.userinfo().realuser().c_str(),
                conn->sasl_server().plain_auth_user().c_str()));
      }
    }
    conn->mutable_user_cred()->set_real_user(conn_context.userinfo().realuser());

    // TODO: Validate effective user when we implement impersonation.
    if (conn_context.userinfo().has_effectiveuser()) {
      conn->mutable_user_cred()->set_effective_user(conn_context.userinfo().effectiveuser());
    }
  }
  return Status::OK();
}

///
/// ClientNegotiationTask
///

ClientNegotiationTask::ClientNegotiationTask(const shared_ptr<Connection>& conn)
  : conn_(conn) {
}

Status ClientNegotiationTask::Run() {
  VLOG(3) << "Client user credentials in negotiation: " << conn_->user_cred().ToString();
  Status s = conn_->sasl_client().Negotiate();
  if (PREDICT_FALSE(!s.ok())) {
    s = s.CloneAndPrepend("Client SASL negotiation failed. " + conn_->ToString());
    LOG(WARNING) << s.ToString();
    return s;
  }
  s = SendConnectionContext(conn_.get());
  if (PREDICT_FALSE(!s.ok())) {
    s = s.CloneAndPrepend("Client connection context negotiation failed. " + conn_->ToString());
    LOG(WARNING) << s.ToString();
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
    s = s.CloneAndPrepend("Server SASL negotiation failed. " + conn_->ToString());
    LOG(WARNING) << s.ToString();
    return s;
  }
  s = RecvConnectionContext(conn_.get());
  if (PREDICT_FALSE(!s.ok())) {
    s = s.CloneAndPrepend("Server connection context negotiation failed. " + conn_->ToString());
    LOG(WARNING) << s.ToString();
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
