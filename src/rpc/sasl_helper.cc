// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "rpc/sasl_helper.h"

#include <set>
#include <string>

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <google/protobuf/message_lite.h>

#include "gutil/endian.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "gutil/map-util.h"
#include "gutil/port.h"
#include "gutil/strings/join.h"
#include "rpc/constants.h"
#include "rpc/rpc_header.pb.h"
#include "rpc/sasl_common.h"
#include "rpc/serialization.h"
#include "util/faststring.h"
#include "util/status.h"

namespace kudu {
namespace rpc {

using google::protobuf::MessageLite;

SaslHelper::SaslHelper(PeerType peer_type)
  : peer_type_(peer_type),
    conn_header_exchanged_(false),
    anonymous_enabled_(false),
    plain_enabled_(false) {
  tag_ = (peer_type_ == SERVER) ? "Sasl Server" : "Sasl Client";
}

SaslHelper::~SaslHelper() {
}

void SaslHelper::set_local_addr(const Sockaddr& addr) {
  local_addr_ = SaslIpPortString(addr);
}
const char* SaslHelper::local_addr_string() const {
  return local_addr_.empty() ? NULL : local_addr_.c_str();
}

void SaslHelper::set_remote_addr(const Sockaddr& addr) {
  remote_addr_ = SaslIpPortString(addr);
}
const char* SaslHelper::remote_addr_string() const {
  return remote_addr_.empty() ? NULL : remote_addr_.c_str();
}

void SaslHelper::set_server_fqdn(const string& domain_name) {
  server_fqdn_ = domain_name;
}
const char* SaslHelper::server_fqdn() const {
  return server_fqdn_.empty() ? NULL : server_fqdn_.c_str();
}

const std::set<std::string>& SaslHelper::GlobalMechs() const {
  if (!global_mechs_) {
    global_mechs_.reset(new set<string>(SaslListAvailableMechs()));
  }
  return *global_mechs_;
}

void SaslHelper::AddToLocalMechList(const string& mech) {
  mechs_.insert(mech);
}

const std::set<std::string>& SaslHelper::LocalMechs() const {
  return mechs_;
}

const char* SaslHelper::LocalMechListString() const {
  JoinStrings(mechs_, " ", &mech_list_);
  return mech_list_.empty() ? NULL : mech_list_.c_str();
}


int SaslHelper::GetOptionCb(const char* plugin_name, const char* option,
                            const char** result, unsigned* len) {
  string cb_name("client_mech_list");
  if (peer_type_ == SERVER) {
    cb_name = "mech_list";
  }

  DVLOG(4) << tag_ << ": GetOption Callback called. ";
  DVLOG(4) << tag_ << ": GetOption Plugin name: " << plugin_name;
  DVLOG(4) << tag_ << ": GetOption Option name: " << option;

  if (PREDICT_FALSE(result == NULL)) {
    LOG(DFATAL) << tag_ << ": SASL Library passed NULL result out-param to GetOption callback!";
    return SASL_BADPARAM;
  }

  if (plugin_name == NULL) {
    // SASL library option, not a plugin option
    if (cb_name == option) {
      *result = LocalMechListString();
      if (len != NULL) *len = strlen(*result);
      DVLOG(3) << tag_ << ": Enabled mech list: " << *result;
      return SASL_OK;
    }
    VLOG(4) << tag_ << ": GetOptionCb: Unknown library option: " << option;
  } else {
    VLOG(4) << tag_ << ": GetOptionCb: Unknown plugin: " << plugin_name;
  }
  return SASL_FAIL;
}

Status SaslHelper::EnableAnonymous() {
  if (PREDICT_FALSE(!ContainsKey(GlobalMechs(), kSaslMechAnonymous))) {
    LOG(DFATAL) << tag_ << ": Unable to find ANONYMOUS SASL plugin";
    return Status::InvalidArgument("Client unable to find ANONYMOUS SASL plugin");
  }
  AddToLocalMechList(kSaslMechAnonymous);
  anonymous_enabled_ = true;
  return Status::OK();
}

bool SaslHelper::IsAnonymousEnabled() const {
  return anonymous_enabled_;
}

Status SaslHelper::EnablePlain() {
  if (PREDICT_FALSE(!ContainsKey(GlobalMechs(), kSaslMechPlain))) {
    LOG(DFATAL) << tag_ << ": Unable to find PLAIN SASL plugin";
    return Status::InvalidArgument("Unable to find PLAIN SASL plugin");
  }
  AddToLocalMechList(kSaslMechPlain);
  plain_enabled_ = true;
  return Status::OK();
}

bool SaslHelper::IsPlainEnabled() const {
  return plain_enabled_;
}

Status SaslHelper::SanityCheckSaslCallId(int32_t call_id) const {
  if (call_id != kSaslCallId) {
    Status s = Status::IllegalState(StringPrintf("Non-SASL request during negotiation. "
          "Expected callId: %d, received callId: %d", kSaslCallId, call_id));
    LOG(DFATAL) << tag_ << ": " << s.ToString();
    return s;
  }
  return Status::OK();
}

Status SaslHelper::ParseSaslMessage(const Slice& param_buf, SaslMessagePB* msg) {
  if (!msg->ParseFromArray(param_buf.data(), param_buf.size())) {
    return Status::IOError(tag_ + ": Invalid SASL message, missing fields",
        msg->InitializationErrorString());
  }
  return Status::OK();
}

Status SaslHelper::SendSaslMessage(Socket* sock, const MessageLite& header, const MessageLite& msg) {
  DCHECK_NOTNULL(sock);
  DCHECK(header.IsInitialized()) << tag_ << ": Header must be initialized";
  DCHECK(msg.IsInitialized()) << tag_ << ": Message must be initialized";

  // Serialize message
  faststring param_buf;
  RETURN_NOT_OK(serialization::SerializeMessage(msg, &param_buf));

  // Serialize header and initial length
  faststring header_buf;
  RETURN_NOT_OK(serialization::SerializeHeader(header, param_buf.size(), &header_buf));

  // Write connection header, if needed
  if (PREDICT_FALSE(peer_type_ == CLIENT && !conn_header_exchanged_)) {
    const uint8_t buflen = kMagicNumberLength + kHeaderFlagsLength;
    uint8_t buf[buflen];
    serialization::SerializeConnHeader(buf);
    size_t nsent;
    RETURN_NOT_OK(sock->BlockingWrite(buf, buflen, &nsent));
    conn_header_exchanged_ = true;
  }

  // Write header & param to stream
  size_t nsent;
  RETURN_NOT_OK(sock->BlockingWrite(header_buf.data(), header_buf.size(), &nsent));
  RETURN_NOT_OK(sock->BlockingWrite(param_buf.data(), param_buf.size(), &nsent));

  return Status::OK();
}

Status SaslHelper::ReceiveFramedMessage(Socket* sock, faststring* recv_buf,
    MessageLite* header, Slice* param_buf) {
  recv_buf->clear();
  recv_buf->resize(kMsgLengthPrefixLength);
  size_t recvd = 0;
  RETURN_NOT_OK(sock->BlockingRecv(recv_buf->data(), kMsgLengthPrefixLength, &recvd));
  uint32_t total_len = NetworkByteOrder::Load32(recv_buf->data());

  recvd = 0;
  recv_buf->resize(total_len + kMsgLengthPrefixLength);
  RETURN_NOT_OK(sock->BlockingRecv(recv_buf->data() + kMsgLengthPrefixLength, total_len, &recvd));
  RETURN_NOT_OK(serialization::ParseMessage(Slice(*recv_buf), header, param_buf));
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
