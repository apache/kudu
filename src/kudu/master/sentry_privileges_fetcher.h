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

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/sentry/sentry_action.h"
#include "kudu/sentry/sentry_authorizable_scope.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/thrift/client.h"
#include "kudu/util/bitset.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/ttl_cache.h"

namespace sentry {
class TListSentryPrivilegesResponse;
class TSentryAuthorizable;
class TSentryPrivilege;
}  // namespace sentry

namespace kudu {
namespace master {

// Utility struct to facilitate evaluating the privileges of a given
// authorizable. This is preferred to using Sentry's Thrift responses directly,
// since useful information has already been parsed to generate this struct
// (e.g. the SentryActions and scope).
// The 'server' field is omitted: everything is implicitly bound to a particular
// Sentry instance which is the only authoritative source of authz information
// for Kudu in the current model of AuthzProvider.
struct AuthorizablePrivileges {
  AuthorizablePrivileges(sentry::SentryAuthorizableScope::Scope scope,
                         std::string db,
                         std::string table,
                         std::string column)
    : all_with_grant(false),
      scope(scope),
      db_name(std::move(db)),
      table_name(std::move(table)),
      column_name(std::move(column)) {
#ifndef NDEBUG
    switch (scope) {
      case sentry::SentryAuthorizableScope::COLUMN:
        CHECK(!column_name.empty());
        FALLTHROUGH_INTENDED;
      case sentry::SentryAuthorizableScope::TABLE:
        CHECK(!table_name.empty());
        FALLTHROUGH_INTENDED;
      case sentry::SentryAuthorizableScope::DATABASE:
        CHECK(!db_name.empty());
        break;
      case sentry::SentryAuthorizableScope::SERVER:
        break;
      default:
        LOG(FATAL) << "not reachable";
    }
#endif
  }

  // Whether the privilege 'ALL' or 'OWNER' has been granted with Sentry's
  // grant option enabled. Note that the grant option can be granted on any
  // action, but for Kudu, we only use it with 'ALL' or 'OWNER'.
  bool all_with_grant;

  // The scope of the authorizable being granted the privileges.
  sentry::SentryAuthorizableScope::Scope scope;

  // The set of actions for which privileges are granted.
  sentry::SentryActionsSet allowed_actions;

  // The fields of the authorizable.
  std::string db_name;
  std::string table_name;
  std::string column_name;
};

// A representation of the Sentry privilege hierarchy branch for a single table
// (including privileges for the table's ancestors and descendents in the
//  authz scope hierarchy) for a single user.
class SentryPrivilegesBranch {
 public:
  // Construct an empty instance: no information on privileges.
  SentryPrivilegesBranch() = default;

  // Construct an instance for the specified 'authorizable' from 'response'.
  SentryPrivilegesBranch(
      const ::sentry::TSentryAuthorizable& authorizable,
      const ::sentry::TListSentryPrivilegesResponse& response);

  // Accessor to the privileges information stored in the object.
  const std::vector<AuthorizablePrivileges>& privileges() const {
    return privileges_;
  }

  // Get estimation on amount of memory used (in bytes) to store this instance.
  size_t memory_footprint() const;

 private:
  // Utility function.
  void DoInit(const ::sentry::TSentryAuthorizable& authorizable,
              const ::sentry::TListSentryPrivilegesResponse& response);

  // Set of privileges are granted.
  std::vector<AuthorizablePrivileges> privileges_;
};

// A utility class to use in SentryAuthzProvider. This class provides an
// interface for finding privileges granted to a user at some authz scope.
// The authoritative source of the authz privileges information is Sentry,
// where the Sentry-related parameters are specified via command line flags for
// kudu-master binary (see the .cc file for available command line flags).
//
// Optionally, the fetcher can use TTL-based cache to store information
// retrieved from Sentry, making it possible to reuse once fetched information
// until corresponding cache entries expire.
class SentryPrivilegesFetcher {
 public:
  explicit SentryPrivilegesFetcher(scoped_refptr<MetricEntity> metric_entity);
  ~SentryPrivilegesFetcher() = default;

  // Start/stop the underlying Sentry client.
  Status Start();
  void Stop();

  // Fetches the user's privileges from Sentry for the authorizable specified
  // by the given table and scope. The result privileges might be served
  // from the cache, if caching is enabled and corresponding entry exists
  // in the cache.
  Status GetSentryPrivileges(
      sentry::SentryAuthorizableScope::Scope requested_scope,
      const std::string& table_name,
      const std::string& user,
      SentryPrivilegesBranch* privileges);

 private:
  friend class SentryAuthzProviderFilterPrivilegesTest;
  friend class SentryAuthzProviderTest;
  friend class SentryPrivilegesBranch;
  FRIEND_TEST(SentryPrivilegesFetcherStaticTest, TestPrivilegesWellFormed);

  // Utility function to determine whether the given privilege is a well-formed
  // possibly Kudu-related privilege describing a descendent or ancestor of the
  // requested authorizable in the Sentry hierarchy tree, i.e. it:
  // - has a Kudu-related action (e.g. ALL, INSERT, UPDATE, etc.),
  // - has a Kudu-related authorizable scope (e.g. SERVER, DATABASE, etc.),
  // - all fields of equal or higher scope to the privilege's scope are set;
  //   none lower are set, and
  // - all fields that are set match those set by the input authorizable.
  static bool SentryPrivilegeIsWellFormed(
      const ::sentry::TSentryPrivilege& privilege,
      const ::sentry::TSentryAuthorizable& requested_authorizable,
      sentry::SentryAuthorizableScope::Scope* scope,
      sentry::SentryAction::Action* action);

  // Returns the set of scope fields expected to be non-empty in a Sentry
  // response with the given authorizable scope. All fields of equal or higher
  // scope are expected to be set.
  static const sentry::AuthorizableScopesSet& ExpectedNonEmptyFields(
      sentry::SentryAuthorizableScope::Scope scope);

  // Returns the set of scope fields expected to be empty in a Sentry response
  // with the given authorizable scope. All fields of lower scope are expected
  // to be empty.
  static const sentry::AuthorizableScopesSet& ExpectedEmptyFields(
      sentry::SentryAuthorizableScope::Scope scope);

  // Sends a request to fetch privileges from Sentry for the given authorizable.
  Status FetchPrivilegesFromSentry(
      const std::string& service_name,
      const std::string& user,
      const ::sentry::TSentryAuthorizable& authorizable,
      SentryPrivilegesBranch* result);

  // Resets the authz cache. In addition to lifecycle-related methods like
  // Start(), this method is also used by tests: if the authz information
  // has been updated by the test, the cache needs to be invalidated.
  //
  // NOTE: this method is not thread-safe and should not be called along with
  //       concurrent authz requests.
  Status ResetCache();

  // Metric entity for registering metric gauges/counters.
  scoped_refptr<MetricEntity> metric_entity_;

  // The authz scope hierarchy level that defines the narrowest authz scope
  // for requests sent to Sentry. If not set, no broadening of authz privilege
  // scope is made.
  boost::optional<sentry::SentryAuthorizableScope> scope_depth_limit_;

  // Client instance to communicate with Sentry.
  thrift::HaClient<sentry::SentryClient> sentry_client_;

  // The TTL cache to store information on privileges received from Sentry.
  typedef TTLCache<std::string, SentryPrivilegesBranch> AuthzInfoCache;
  std::unique_ptr<AuthzInfoCache> cache_;

  // Utility dictionary to keep track of requests sent to Sentry. Access is
  // guarded by pending_requests_lock_. The key corresponds to the set of
  // parameters for a request sent to Sentry.
  struct SentryRequestsInfo {
    std::vector<StatusCallback> callbacks;
    std::shared_ptr<SentryPrivilegesBranch> result;
  };
  std::unordered_map<std::string, SentryRequestsInfo> pending_requests_;
  simple_spinlock pending_requests_lock_;
};

} // namespace master
} // namespace kudu
