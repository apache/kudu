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

#include "kudu/rpc/sasl_helper.h"

#include <string>

#include <glog/logging.h>
#include <google/protobuf/message_lite.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/serialization.h"
#include "kudu/util/status.h"

using std::string;

namespace kudu {
namespace rpc {

using google::protobuf::MessageLite;

SaslHelper::SaslHelper(PeerType peer_type)
  : peer_type_(peer_type),
    global_mechs_(SaslListAvailableMechs()),
    plain_enabled_(false),
    gssapi_enabled_(false) {
  tag_ = (peer_type_ == SERVER) ? "Server" : "Client";
}

void SaslHelper::set_local_addr(const Sockaddr& addr) {
  local_addr_ = SaslIpPortString(addr);
}
const char* SaslHelper::local_addr_string() const {
  return local_addr_.empty() ? nullptr : local_addr_.c_str();
}

void SaslHelper::set_remote_addr(const Sockaddr& addr) {
  remote_addr_ = SaslIpPortString(addr);
}
const char* SaslHelper::remote_addr_string() const {
  return remote_addr_.empty() ? nullptr : remote_addr_.c_str();
}

void SaslHelper::set_server_fqdn(const string& domain_name) {
  server_fqdn_ = domain_name;
}
const char* SaslHelper::server_fqdn() const {
  return server_fqdn_.empty() ? nullptr : server_fqdn_.c_str();
}

const char* SaslHelper::EnabledMechsString() const {
  JoinStrings(enabled_mechs_, " ", &enabled_mechs_string_);
  return enabled_mechs_string_.c_str();
}

int SaslHelper::GetOptionCb(const char* plugin_name, const char* option,
                            const char** result, unsigned* len) {
  DVLOG(4) << tag_ << ": GetOption Callback called. ";
  DVLOG(4) << tag_ << ": GetOption Plugin name: "
                   << (plugin_name == nullptr ? "NULL" : plugin_name);
  DVLOG(4) << tag_ << ": GetOption Option name: " << option;

  if (PREDICT_FALSE(result == nullptr)) {
    LOG(DFATAL) << tag_ << ": SASL Library passed NULL result out-param to GetOption callback!";
    return SASL_BADPARAM;
  }

  if (plugin_name == nullptr) {
    // SASL library option, not a plugin option
    if (strcmp(option, "mech_list") == 0) {
      *result = EnabledMechsString();
      if (len != nullptr) *len = strlen(*result);
      VLOG(4) << tag_ << ": Enabled mech list: " << *result;
      return SASL_OK;
    }
    VLOG(4) << tag_ << ": GetOptionCb: Unknown library option: " << option;
  } else {
    VLOG(4) << tag_ << ": GetOptionCb: Unknown plugin: " << plugin_name;
  }
  return SASL_FAIL;
}

Status SaslHelper::EnablePlain() {
  RETURN_NOT_OK(EnableMechanism(kSaslMechPlain));
  plain_enabled_ = true;
  return Status::OK();
}

Status SaslHelper::EnableGSSAPI() {
  RETURN_NOT_OK(EnableMechanism(kSaslMechGSSAPI));
  gssapi_enabled_ = true;
  return Status::OK();
}

Status SaslHelper::EnableMechanism(const string& mech) {
  if (PREDICT_FALSE(!ContainsKey(global_mechs_, mech))) {
    return Status::InvalidArgument("unable to find SASL plugin", mech);
  }
  enabled_mechs_.insert(mech);
  return Status::OK();
}

bool SaslHelper::IsPlainEnabled() const {
  return plain_enabled_;
}

Status SaslHelper::CheckNegotiateCallId(int32_t call_id) const {
  if (call_id != kNegotiateCallId) {
    Status s = Status::IllegalState(strings::Substitute(
        "Received illegal call-id during negotiation; expected: $0, received: $1",
        kNegotiateCallId, call_id));
    LOG(DFATAL) << tag_ << ": " << s.ToString();
    return s;
  }
  return Status::OK();
}

Status SaslHelper::ParseNegotiatePB(const Slice& param_buf, NegotiatePB* msg) {
  if (!msg->ParseFromArray(param_buf.data(), param_buf.size())) {
    return Status::IOError(tag_ + ": Invalid SASL message, missing fields",
        msg->InitializationErrorString());
  }
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
