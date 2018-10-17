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

#include "kudu/sentry/sentry_client.h"

#include <exception>
#include <memory>
#include <ostream>
#include <string>

#include <glog/logging.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TMultiplexedProtocol.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TTransport.h>
#include <thrift/transport/TTransportException.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/sentry/sentry_common_service_constants.h"
#include "kudu/sentry/sentry_common_service_types.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/thrift/client.h"
#include "kudu/thrift/sasl_client_transport.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

using apache::thrift::TException;
using apache::thrift::protocol::TMultiplexedProtocol;
using apache::thrift::transport::TTransportException;
using kudu::thrift::CreateClientProtocol;
using kudu::thrift::SaslException;
using sentry::SentryPolicyServiceClient;
using sentry::TAlterSentryRoleAddGroupsRequest;
using sentry::TAlterSentryRoleAddGroupsResponse;
using sentry::TAlterSentryRoleGrantPrivilegeRequest;
using sentry::TAlterSentryRoleGrantPrivilegeResponse;
using sentry::TCreateSentryRoleRequest;
using sentry::TCreateSentryRoleResponse;
using sentry::TDropSentryRoleRequest;
using sentry::TDropSentryRoleResponse;
using sentry::TListSentryPrivilegesRequest;
using sentry::TListSentryPrivilegesResponse;
using sentry::TSentryResponseStatus;
using sentry::g_sentry_common_service_constants;
using std::make_shared;
using strings::Substitute;

namespace kudu {
namespace sentry {

const uint16_t SentryClient::kDefaultSentryPort = 8038;

const char* const SentryClient::kServiceName = "Sentry";

const int kSlowExecutionWarningThresholdMs = 1000;

namespace {
Status ConvertStatus(const TSentryResponseStatus& status) {
  // A switch isn't possible because these values aren't generated as real constants.
  if (status.value == g_sentry_common_service_constants.TSENTRY_STATUS_OK) {
    return Status::OK();
  }
  if (status.value == g_sentry_common_service_constants.TSENTRY_STATUS_ALREADY_EXISTS) {
    return Status::AlreadyPresent(status.message, status.stack);
  }
  if (status.value == g_sentry_common_service_constants.TSENTRY_STATUS_NO_SUCH_OBJECT) {
    return Status::NotFound(status.message, status.stack);
  }
  if (status.value == g_sentry_common_service_constants.TSENTRY_STATUS_RUNTIME_ERROR) {
    return Status::RuntimeError(status.message, status.stack);
  }
  if (status.value == g_sentry_common_service_constants.TSENTRY_STATUS_INVALID_INPUT) {
    return Status::InvalidArgument(status.message, status.stack);
  }
  if (status.value == g_sentry_common_service_constants.TSENTRY_STATUS_ACCESS_DENIED) {
    return Status::NotAuthorized(status.message, status.stack);
  }
  if (status.value == g_sentry_common_service_constants.TSENTRY_STATUS_THRIFT_VERSION_MISMATCH) {
    return Status::NotSupported(status.message, status.stack);
  }
  LOG(WARNING) << "Unknown error code in Sentry status: " << status;
  return Status::RuntimeError(
      Substitute("unknown error code: $0: $1", status.value, status.message), status.stack);
}
} // namespace

// Wraps calls to the Sentry Thrift service, catching any exceptions and
// converting the response status to a Kudu Status. If there is no
// response_status then {} can be substituted.
#define SENTRY_RET_NOT_OK(call, response_status, msg) \
  try { \
    (call); \
    RETURN_NOT_OK_PREPEND(ConvertStatus(response_status), (msg)); \
  } catch (const SaslException& e) { \
    return e.status().CloneAndPrepend(msg); \
  } catch (const TTransportException& e) { \
    switch (e.getType()) { \
      case TTransportException::TIMED_OUT: return Status::TimedOut((msg), e.what()); \
      case TTransportException::BAD_ARGS: return Status::InvalidArgument((msg), e.what()); \
      case TTransportException::CORRUPTED_DATA: return Status::Corruption((msg), e.what()); \
      default: return Status::NetworkError((msg), e.what()); \
    } \
  } catch (const TException& e) { \
    return Status::IOError((msg), e.what()); \
  } catch (const std::exception& e) { \
    return Status::RuntimeError((msg), e.what()); \
  }

SentryClient::SentryClient(const HostPort& address, const thrift::ClientOptions& options)
      : client_(SentryPolicyServiceClient(
            make_shared<TMultiplexedProtocol>(CreateClientProtocol(address, options),
                                              "SentryPolicyService"))) {
}

SentryClient::~SentryClient() {
  WARN_NOT_OK(Stop(), "failed to shutdown Sentry client");
}

Status SentryClient::Start() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "starting Sentry client");
  SENTRY_RET_NOT_OK(client_.getOutputProtocol()->getTransport()->open(),
                    {}, "failed to open Sentry connection");
  return Status::OK();
}

Status SentryClient::Stop() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "stopping Sentry client");
  SENTRY_RET_NOT_OK(client_.getInputProtocol()->getTransport()->close(),
                    {}, "failed to close Sentry connection");
  return Status::OK();
}

bool SentryClient::IsConnected() {
  return client_.getInputProtocol()->getTransport()->isOpen();
}

Status SentryClient::CreateRole(const TCreateSentryRoleRequest& request) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "create Sentry role");
  TCreateSentryRoleResponse response;
  SENTRY_RET_NOT_OK(client_.create_sentry_role(response, request),
                    response.status, "failed to create Sentry role");
  return Status::OK();
}

Status SentryClient::DropRole(const TDropSentryRoleRequest& request) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "drop Sentry role");
  TDropSentryRoleResponse response;
  SENTRY_RET_NOT_OK(client_.drop_sentry_role(response, request),
                    response.status, "failed to drop Sentry role");
  return Status::OK();
}

Status SentryClient::ListPrivilegesByUser(const TListSentryPrivilegesRequest& request,
                                          TListSentryPrivilegesResponse* response)  {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs,
                            "list Sentry privilege by user");
  SENTRY_RET_NOT_OK(client_.list_sentry_privileges_by_user_and_itsgroups(*response, request),
                    response->status, "failed to list Sentry privilege by user");
  return Status::OK();
}

Status SentryClient::AlterRoleAddGroups(const TAlterSentryRoleAddGroupsRequest& request,
                                        TAlterSentryRoleAddGroupsResponse* response)  {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs,
                            "alter Sentry role add groups");
  SENTRY_RET_NOT_OK(client_.alter_sentry_role_add_groups(*response, request),
                    response->status, "failed to alter Sentry role add groups");
  return Status::OK();
}

Status SentryClient::AlterRoleGrantPrivilege(const TAlterSentryRoleGrantPrivilegeRequest& request,
                                             TAlterSentryRoleGrantPrivilegeResponse* response)  {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs,
                            "alter Sentry role grant privileges");
  SENTRY_RET_NOT_OK(client_.alter_sentry_role_grant_privilege(*response, request),
                    response->status, "failed to alter Sentry role grant privileges");
  return Status::OK();
}

} // namespace sentry
} // namespace kudu
