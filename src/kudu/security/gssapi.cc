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

#include "kudu/security/gssapi.h"

#include <cstring>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/strings/escaping.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"

using std::string;

namespace kudu {
namespace gssapi {

namespace {

string ErrorToString(OM_uint32 code, OM_uint32 type) {
  string description;
  OM_uint32 message_context = 0;

  do {
    if (!description.empty()) {
      description.append(": ");
    }
    OM_uint32 minor = 0;
    gss_buffer_desc buf;
    gss_display_status(&minor, code, type, GSS_C_NULL_OID, &message_context, &buf);
    const char* err = static_cast<const char*>(buf.value);
    // The error message buffer returned has an explicit length, but in some cases
    // this length appears to include the null terminator character, and in others
    // it doesn't. So, we trim off any null terminators if necessary.
    int err_len = strnlen(err, buf.length);
    description.append(err, err_len);

    // NOTE: call gss_release_buffer explicitly here instead of ReleaseBufferOrWarn
    // to avoid the potential for infinite recursion back into ErrorToString.
    gss_release_buffer(&minor, &buf);
  } while (message_context != 0);

  return description;
}

// Wrap a call to F(...), which is typically one of the gss_* functions from
// the gssapi library. These functions all return a major status code and
// also provide a minor status code as their first out-param.
template <class F, class... Args>
Status WrapGssCall(F func, Args&&... args) {
  OM_uint32 minor = 0;
  OM_uint32 major = func(&minor, std::forward<Args>(args)...);
  return MajorMinorToStatus(major, minor);
}

void ReleaseNameOrWarn(gss_name_t name) {
  if (name == GSS_C_NO_NAME) return;
  WARN_NOT_OK(WrapGssCall(gss_release_name, &name), "Unable to release GSS name");
}

void ReleaseBufferOrWarn(gss_buffer_t buf) {
  if (buf == GSS_C_NO_BUFFER) return;
  WARN_NOT_OK(WrapGssCall(gss_release_buffer, buf), "Unable to release GSS buffer");
}

void ReleaseContextOrWarn(gss_ctx_id_t ctx) {
  if (ctx == GSS_C_NO_CONTEXT) return;
  WARN_NOT_OK(WrapGssCall(gss_delete_sec_context, &ctx, GSS_C_NO_BUFFER),
              "Unable to release GSS context");
}

} // anonymous namespace


Status MajorMinorToStatus(OM_uint32 major, OM_uint32 minor) {
  if (GSS_ERROR(major)) {
    string maj_str = ErrorToString(major, GSS_C_GSS_CODE);
    string min_str = minor != 0 ? ErrorToString(minor, GSS_C_MECH_CODE) : "";
    return Status::NotAuthorized(maj_str, min_str);
  }
  if (major == GSS_S_CONTINUE_NEEDED) {
    return Status::Incomplete("");
  }

  return Status::OK();
}


Status SpnegoStep(const string& in_token_b64,
                  string* out_token_b64,
                  bool* complete,
                  string* authenticated_principal) {
  string token;
  if (!strings::Base64Unescape(in_token_b64, &token)) {
    return Status::InvalidArgument("invalid base64 encoding for token");
  }

  // Workaround MIT krb5 bug [1] fixed in krb5 1.16 and 1.15.3:
  //
  // gssint_get_mech_type_oid() was missing some length verification that could
  // cause reads past the end of the input token. So, we extend the actual
  // allocation of the input token an extra 256 bytes of padding.
  //
  // Without this fix, our ASAN builds fail with an out-of-bounds read.
  //
  // [1] http://krbdev.mit.edu/rt/Ticket/History.html?id=8620
  size_t real_token_size = token.size();
  token.resize(real_token_size + 256);

  gss_buffer_desc input_token {real_token_size, const_cast<char*>(token.data())};

  gss_ctx_id_t ctx = GSS_C_NO_CONTEXT;
  gss_name_t client_name = GSS_C_NO_NAME;
  SCOPED_CLEANUP({ ReleaseNameOrWarn(client_name); });

  gss_buffer_desc out_token {0, nullptr};
  SCOPED_CLEANUP({ ReleaseBufferOrWarn(&out_token); });
  Status s = WrapGssCall(gss_accept_sec_context, &ctx, GSS_C_NO_CREDENTIAL,
                         &input_token, GSS_C_NO_CHANNEL_BINDINGS,
                         &client_name, /*mech_type=*/ nullptr,
                         &out_token, /*ret_flags=*/nullptr,
                         /*time_rec=*/nullptr, /*delegated_cred_handle=*/nullptr);
  SCOPED_CLEANUP({ ReleaseContextOrWarn(ctx); });
  if (!s.ok() && !s.IsIncomplete()) {
    return s;
  }
  *complete = s.ok();

  if (*complete) {
    gss_buffer_desc name_buf {0, nullptr};
    SCOPED_CLEANUP({ ReleaseBufferOrWarn(&name_buf); });
    RETURN_NOT_OK_PREPEND(WrapGssCall(gss_display_name, client_name, &name_buf, nullptr),
                          "Unable to extract authenticated principal name");
    authenticated_principal->assign(reinterpret_cast<char*>(name_buf.value),
                                    name_buf.length);
  }

  string out_token_str(reinterpret_cast<char*>(out_token.value),
                       out_token.length);
  strings::Base64Escape(out_token_str, out_token_b64);

  return Status::OK();
}


} // namespace gssapi
} // namespace kudu
