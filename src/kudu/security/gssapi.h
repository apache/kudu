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
#pragma once

#include <string>

#include <gssapi/gssapi.h>

namespace kudu {

class Status;

namespace gssapi {

// Convert the given major/minor GSSAPI error codes into a Status.
Status MajorMinorToStatus(OM_uint32 major, OM_uint32 minor);

// Run a step of SPNEGO authentication.
//
// 'in_token_b64' is the base64-encoded token provided by the client, which may be empty
// if the client did not provide any such token (e.g. if the HTTP 'Authorization' header
// was not present).

// 'out_token_b64' is the base64-encoded output token to send back to the client
// during this round of negotiation.
//
// If any error occurs (eg an invalid token is provided), a bad Status is returned.
//
// An OK status indicates that the negotiation is proceeding successfully, or has
// completed, whereas a non-OK status indicates an error or an unsuccessful
// authentication (in which case the out-parameters will not be modified).
//
// In the case of an OK status, '*complete' indicates whether any further rounds are
// required. On completion of negotiation, 'authenticated_principal' will be set to the
// full principal name of the remote user.
//
// NOTE: per the SPNEGO protocol, the final "complete" negotiation stage may
// include a token.
Status SpnegoStep(const std::string& in_token_b64,
                  std::string* out_token_b64,
                  bool* complete,
                  std::string* authenticated_principal);

} // namespace gssapi
} // namespace kudu
