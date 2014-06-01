// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "rpc/inbound_call.h"

#include <tr1/memory>
#include <vector>

#include "rpc/connection.h"
#include "rpc/rpc_introspection.pb.h"
#include "rpc/serialization.h"
#include "util/metrics.h"
#include "util/trace.h"

using google::protobuf::FieldDescriptor;
using google::protobuf::Message;
using google::protobuf::MessageLite;
using google::protobuf::io::CodedOutputStream;
using std::tr1::shared_ptr;
using std::vector;

DEFINE_bool(rpc_dump_all_traces, false,
            "If true, dump all RPC traces at INFO level");

namespace kudu {
namespace rpc {

InboundCall::InboundCall(Connection* conn)
  : conn_(conn),
    trace_(new Trace) {
  RecordCallReceived();
}

InboundCall::~InboundCall() {}

Status InboundCall::ParseFrom(gscoped_ptr<InboundTransfer> transfer) {
  RETURN_NOT_OK(serialization::ParseMessage(transfer->data(), &header_, &serialized_request_));
  // retain the buffer that we have a view into
  transfer_.swap(transfer);
  return Status::OK();
}

void InboundCall::RespondSuccess(const MessageLite& response) {
  Respond(response, true);
}

void InboundCall::RespondFailure(ErrorStatusPB::RpcErrorCodePB error_code,
                                 const Status& status) {
  ErrorStatusPB err;
  err.set_message(status.ToString());

  Respond(err, false);
}

void InboundCall::RespondApplicationError(int error_ext_id, const std::string& message,
                                          const MessageLite& app_error_pb) {
  ErrorStatusPB err;
  err.set_message(message);

  const FieldDescriptor* app_error_field =
    err.GetReflection()->FindKnownExtensionByNumber(error_ext_id);
  if (app_error_field != NULL) {
    err.GetReflection()->MutableMessage(&err, app_error_field)->CheckTypeAndMergeFrom(app_error_pb);
  } else {
    LOG(DFATAL) << "Unable to find application error extension ID " << error_ext_id
                << " (message=" << message << ")";
  }

  Respond(err, false);
}

void InboundCall::Respond(const MessageLite& response,
                          bool is_success) {
  Status s = SerializeResponseBuffer(response, is_success);
  if (PREDICT_FALSE(!s.ok())) {
    // TODO: test error case, serialize error response instead
    LOG(DFATAL) << "Unable to serialize response: " << s.ToString();
  }

  TRACE_TO(trace_, "Queueing $0 response", is_success ? "success" : "failure");

  LogTrace();
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

void InboundCall::SerializeResponseTo(vector<Slice>* slices) const {
  CHECK_GT(response_hdr_buf_.size(), 0);
  CHECK_GT(response_msg_buf_.size(), 0);
  slices->push_back(Slice(response_hdr_buf_));
  slices->push_back(Slice(response_msg_buf_));
}

string InboundCall::ToString() const {
  return StringPrintf("Call %s from %s (#%d)", method_name().c_str(),
                      conn_->remote().ToString().c_str(),
                      header_.call_id());
}

void InboundCall::DumpPB(const DumpRunningRpcsRequestPB& req,
                         RpcCallInProgressPB* resp) {
  resp->mutable_header()->CopyFrom(header_);
  if (req.include_traces() && trace_) {
    resp->set_trace_buffer(trace_->DumpToString(true));
  }
  resp->set_micros_elapsed(MonoTime::Now(MonoTime::FINE).GetDeltaSince(timing_.time_received)
                           .ToMicroseconds());
}

void InboundCall::LogTrace() const {
  MonoTime now = MonoTime::Now(MonoTime::FINE);
  int total_time = now.GetDeltaSince(timing_.time_received).ToMilliseconds();

  if (header_.has_timeout_millis() && header_.timeout_millis() > 0) {
    double log_threshold = header_.timeout_millis() * 0.75f;
    if (total_time > log_threshold) {
      // TODO: consider pushing this onto another thread since it may be slow.
      // The traces may also be too large to fit in a log message.
      LOG(WARNING) << ToString() << " took " << total_time << "ms (client timeout "
                   << header_.timeout_millis() << ").";
      std::string s = trace_->DumpToString(true);
      if (!s.empty()) {
        LOG(WARNING) << "Trace:\n" << s;
      }
      return;
    }
  }

  if (PREDICT_FALSE(FLAGS_rpc_dump_all_traces)) {
    LOG(INFO) << ToString() << " took " << total_time << "ms. Trace:";
    trace_->Dump(&LOG(INFO), true);
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

void InboundCall::RecordCallReceived() {
  DCHECK(!timing_.time_received.Initialized());  // Protect against multiple calls.
  timing_.time_received = MonoTime::Now(MonoTime::FINE);
}

void InboundCall::RecordHandlingStarted(Histogram* incoming_queue_time) {
  DCHECK(incoming_queue_time != NULL);
  DCHECK(!timing_.time_handled.Initialized());  // Protect against multiple calls.
  timing_.time_handled = MonoTime::Now(MonoTime::FINE);
  incoming_queue_time->Increment(
      timing_.time_handled.GetDeltaSince(timing_.time_received).ToMicroseconds());
}

void InboundCall::RecordHandlingCompleted(Histogram* handler_run_time) {
  DCHECK(handler_run_time != NULL);
  DCHECK(!timing_.time_completed.Initialized());  // Protect against multiple calls.
  timing_.time_completed = MonoTime::Now(MonoTime::FINE);
  handler_run_time->Increment(
      timing_.time_completed.GetDeltaSince(timing_.time_handled).ToMicroseconds());
}

bool InboundCall::ClientTimedOut() const {
  if (!header_.has_timeout_millis() || header_.timeout_millis() == 0) {
    return false;
  }

  MonoTime now = MonoTime::Now(MonoTime::FINE);
  int total_time = now.GetDeltaSince(timing_.time_received).ToMilliseconds();
  return total_time > header_.timeout_millis();
}

} // namespace rpc
} // namespace kudu
