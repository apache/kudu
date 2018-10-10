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

#include <cstdint>

#include "kudu/gutil/port.h"
#include "kudu/sentry/SentryPolicyService.h"
#include "kudu/util/status.h"

namespace sentry {
class TAlterSentryRoleAddGroupsRequest;
class TAlterSentryRoleAddGroupsResponse;
class TAlterSentryRoleGrantPrivilegeRequest;
class TAlterSentryRoleGrantPrivilegeResponse;
class TCreateSentryRoleRequest;
class TDropSentryRoleRequest;
class TListSentryPrivilegesRequest;
class TListSentryPrivilegesResponse;
}

namespace kudu {

class HostPort;

namespace thrift {
struct ClientOptions;
} // namespace thrift

namespace sentry {

// A client for a Sentry service.
//
// All operations are synchronous, and may block.
//
// SentryClient is not thread safe.
//
// SentryClient wraps a single TCP connection to an HMS, and does not attempt to
// retry on failure. If higher-level features like thread-safety, retrying, and
// HA support are needed then use thrift::HaClient<SentryClient> to wrap the HMS
// client.
class SentryClient {
 public:

  static const uint16_t kDefaultSentryPort;

  static const char* const kServiceName;

  // Create a SentryClient connection to the provided Sentry service Thrift RPC address.
  SentryClient(const HostPort& address, const thrift::ClientOptions& options);
  ~SentryClient();

  // Starts the Sentry service client.
  //
  // This method will open a synchronous TCP connection to the Sentry service.
  // If the Sentry service can not be reached within the connection timeout
  // interval, an error is returned.
  //
  // Must be called before any subsequent operations using the client.
  Status Start() WARN_UNUSED_RESULT;

  // Stops the Sentry service client.
  //
  // This is optional; if not called the destructor will stop the client.
  Status Stop() WARN_UNUSED_RESULT;

  // Returns 'true' if the client is connected to the remote server.
  bool IsConnected() WARN_UNUSED_RESULT;

  // Creates a new role in Sentry.
  Status CreateRole(const ::sentry::TCreateSentryRoleRequest& request) WARN_UNUSED_RESULT;

  // Drops a role in Sentry.
  Status DropRole(const ::sentry::TDropSentryRoleRequest& request) WARN_UNUSED_RESULT;

  // List Sentry privileges by user.
  Status ListPrivilegesByUser(const ::sentry::TListSentryPrivilegesRequest& request,
      ::sentry::TListSentryPrivilegesResponse* response) WARN_UNUSED_RESULT;

  // Alter role to add groups in Sentry.
  Status AlterRoleAddGroups(const ::sentry::TAlterSentryRoleAddGroupsRequest& request,
      ::sentry::TAlterSentryRoleAddGroupsResponse* response) WARN_UNUSED_RESULT;

  // Alter role to grant privileges in Sentry.
  Status AlterRoleGrantPrivilege(const ::sentry::TAlterSentryRoleGrantPrivilegeRequest& request,
     ::sentry::TAlterSentryRoleGrantPrivilegeResponse* response) WARN_UNUSED_RESULT;

private:
  ::sentry::SentryPolicyServiceClient client_;
};

} // namespace sentry
} // namespace kudu
