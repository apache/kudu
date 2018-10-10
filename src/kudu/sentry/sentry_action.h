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

#include "kudu/util/status.h"

namespace kudu {
namespace sentry {

// A replication of Sentry Action, which is the operation taken
// on an authorizable/object. In this case, HiveSQL model is chosen
// to define the actions. One action can imply another following rules
// defined in Imply().
//
// This class is not thread-safe.
class SentryAction {
 public:
  static const char* const kWildCard;

  // Actions that are supported. All actions are independent,
  // except that ALL subsumes every other action, and every
  // action subsumes METADATA. OWNER is a special action that
  // behaves like the ALL.
  // Note that 'UNINITIALIZED' is not an actual operation but
  // only to represent an action in uninitialized state.
  //
  // See org.apache.sentry.core.model.db.HiveActionFactory.
  enum class Action {
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

  SentryAction();

  explicit SentryAction(Action action);

  Action action() const {
    return action_;
  }

  // Create an Action from string.
  Status FromString(const std::string& action);

  // Check if an action implies the other. In general,
  //   1. an action only implies itself.
  //   2. with the exceptions that ALL, OWNER imply all other actions,
  //      and any action implies METADATA.
  //
  // See org.apache.sentry.policy.common.CommonPrivilege.impliesAction.
  bool Imply(const SentryAction& other) const;

 private:
  Action action_;
};

} // namespace sentry
} // namespace kudu
