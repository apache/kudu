// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "rpc/inbound_call.h"

#include <tr1/memory>
#include <vector>

#include "rpc/connection.h"
#include "rpc/serialization.h"
#include "util/trace.h"

using google::protobuf::Message;
using google::protobuf::MessageLite;
using google::protobuf::io::CodedOutputStream;
using std::tr1::shared_ptr;
using std::vector;

namespace kudu { namespace rpc {

InboundCall::InboundCall(const shared_ptr<Connection> &conn)
  : conn_(conn),
    receive_timestamp_(MonoTime::Now(MonoTime::FINE)),
    trace_(new Trace) {
}

InboundCall::~InboundCall() {}

Status InboundCall::ParseFrom(gscoped_ptr<InboundTransfer> transfer) {
  RETURN_NOT_OK(serialization::ParseMessage(transfer->data(), &header_, &serialized_request_));
  // retain the buffer that we have a view into
  transfer_.swap(transfer);
  return Status::OK();
}

void InboundCall::RespondSuccess(const MessageLite& response) {
  Status s = SerializeResponseBuffer(response, true);
  if (PREDICT_FALSE(!s.ok())) {
    // TODO: test error case, serialize error response instead
    LOG(DFATAL) << "Unable to serialize response: " << s.ToString();
  }

  trace_->Message("Queueing success response");
  LogIfSlow();
  conn_->QueueResponseForCall(gscoped_ptr<InboundCall>(this).Pass());
}

void InboundCall::RespondFailure(const Status& status) {
  ErrorStatusPB err;
  err.set_message(status.ToString());

  Status s = SerializeResponseBuffer(err, false);
  if (PREDICT_FALSE(!s.ok())) {
    // TODO: test error case, serialize error response instead
    LOG(DFATAL) << "Unable to serialize response: " << s.ToString();
  }

  trace_->Message("Queueing failure response");
  LogIfSlow();
  conn_->QueueResponseForCall(gscoped_ptr<InboundCall>(this).Pass());
}

Status InboundCall::SerializeResponseBuffer(const MessageLite& response,
                                            bool is_success) {

  RETURN_NOT_OK(serialization::SerializeMessage(response, &response_msg_buf_));

  ResponseHeader resp_hdr;
  resp_hdr.set_call_id(header_.call_id());
  resp_hdr.set_is_error(!is_success);

  RETURN_NOT_OK(serialization::SerializeHeader(resp_hdr, response_msg_buf_.size(),
      &response_hdr_buf_));

  return Status::OK();
}

Status InboundCall::SerializeResponseTo(vector<Slice>* slices) const {
  CHECK_GT(response_hdr_buf_.size(), 0);
  CHECK_GT(response_msg_buf_.size(), 0);
  slices->push_back(Slice(response_hdr_buf_));
  slices->push_back(Slice(response_msg_buf_));
  return Status::OK();
}

string InboundCall::ToString() const {
  return StringPrintf("Call %s from %s (#%d)", method_name().c_str(),
                      conn_->remote().ToString().c_str(),
                      header_.call_id());
}

void InboundCall::LogIfSlow() const {
  if (!header_.has_timeout_millis() || header_.timeout_millis() == 0) return;

  double log_threshold = header_.timeout_millis() * 0.75f;

  MonoTime now = MonoTime::Now(MonoTime::FINE);
  int total_time = now.GetDeltaSince(receive_timestamp_).ToMilliseconds();

  if (total_time > log_threshold) {
    // TODO: consider pushing this onto another thread since it may be slow.
    // The traces may also be too large to fit in a log message.
    LOG(WARNING) << ToString() << " took " << total_time << "ms (client timeout "
                 << header_.timeout_millis() << ").";
    std::stringstream stream;
    trace_->Dump(&stream);
    std::string s = stream.str();
    if (!s.empty()) {
      LOG(WARNING) << "Trace:\n" << s;
    }
  }
}

const UserCredentials& InboundCall::user_credentials() const {
  return conn_->user_credentials();
}

const Sockaddr& InboundCall::remote_address() const {
  return conn_->remote();
}

Trace* InboundCall::trace() {
  return trace_.get();
}

} // namespace rpc
} // namespace kudu
