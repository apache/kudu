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

#include <iosfwd>
#include <string>

#include "kudu/gutil/port.h"
#include "kudu/util/bitset.h"
#include "kudu/util/status.h"

namespace kudu {
namespace sentry {

// A carbon copy of Sentry Action, which is the operation taken
// on an authorizable, and authorizable is linear hierarchical
// structured resource. In this case, HiveSQL privilege model is
// chosen to define authorizables (server → database → table → column)
// and actions (create, drop, etc.) to authorize users' operations
// (e.g. create a table, drop a database).
// See org.apache.sentry.core.model.db.HivePrivilegeModel.
//
// One action can imply another following rules defined in Implies().
class SentryAction {
 public:
  static const char* const kWildCard;

  // Actions that are supported. All actions are independent,
  // except that ALL subsumes every other action, and every
  // action subsumes METADATA. OWNER is a special action that
  // behaves like ALL.
  // Note that 'UNINITIALIZED' is not an actual operation but
  // only to represent an action in uninitialized state.
  //
  // See org.apache.sentry.core.model.db.HiveActionFactory.
  enum Action {
    UNINITIALIZED,
    ALL,
    METADATA,
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    ALTER,
    CREATE,
    DROP,
    OWNER,
  };
  static const size_t kMaxAction = Action::OWNER + 1;

  // The default constructor is useful when creating an Action
  // from string.
  SentryAction();

  explicit SentryAction(Action action);

  Action action() const {
    return action_;
  }

  // Create an Action from string.
  static Status FromString(const std::string& str,
                           SentryAction* action) WARN_UNUSED_RESULT;

  // Check if this action implies 'other'. In general,
  //   1. an action only implies itself.
  //   2. with the exceptions that ALL, OWNER imply all other actions,
  //      and any action implies METADATA.
  //
  // See org.apache.sentry.policy.common.CommonPrivilege.impliesAction.
  bool Implies(const SentryAction& other) const;

 private:
  Action action_;
};

static constexpr const char* const kActionAll = "ALL";
static constexpr const char* const kActionMetadata = "METADATA";
static constexpr const char* const kActionSelect = "SELECT";
static constexpr const char* const kActionInsert = "INSERT";
static constexpr const char* const kActionUpdate = "UPDATE";
static constexpr const char* const kActionDelete = "DELETE";
static constexpr const char* const kActionAlter = "ALTER";
static constexpr const char* const kActionCreate = "CREATE";
static constexpr const char* const kActionDrop = "DROP";
static constexpr const char* const kActionOwner = "OWNER";

const char* ActionToString(SentryAction::Action action);

std::ostream& operator<<(std::ostream& o, SentryAction::Action action);

typedef FixedBitSet<sentry::SentryAction::Action, sentry::SentryAction::kMaxAction>
    SentryActionsSet;

} // namespace sentry
} // namespace kudu
