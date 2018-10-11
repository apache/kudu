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

#include "kudu/sentry/sentry_action.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::vector;

namespace kudu {

namespace sentry {

TEST(SentryActionTest, TestImplyAction) {
  SentryAction all(SentryAction::Action::ALL);
  SentryAction metadata(SentryAction::Action::METADATA);
  SentryAction select(SentryAction::Action::SELECT);
  SentryAction insert(SentryAction::Action::INSERT);
  SentryAction update(SentryAction::Action::UPDATE);
  SentryAction del(SentryAction::Action::DELETE);
  SentryAction alter(SentryAction::Action::ALTER);
  SentryAction create(SentryAction::Action::CREATE);
  SentryAction drop(SentryAction::Action::DROP);
  SentryAction owner(SentryAction::Action::OWNER);

  // Different action cannot imply each other.
  ASSERT_FALSE(insert.Implies(select));
  ASSERT_FALSE(select.Implies(insert));

  vector<SentryAction> actions({ all, select, insert, update,
                                 del, alter, create, drop, owner });

  // Any action subsumes METADATA, not vice versa.
  for (const auto& action : actions) {
    ASSERT_TRUE(action.Implies(metadata));
    ASSERT_FALSE(metadata.Implies(action));
  }

  actions.emplace_back(metadata);
  for (const auto& action : actions) {
    // Action ALL implies all other actions.
    ASSERT_TRUE(all.Implies(action));

    // Action OWNER equals to ALL, which implies all other actions.
    ASSERT_TRUE(owner.Implies(action));

    // Any action implies itself.
    ASSERT_TRUE(action.Implies(action));
  }
}

TEST(SentryActionTest, TestFromString) {
  // Action '*' equals to ALL.
  SentryAction wildcard;
  ASSERT_OK(SentryAction::FromString(SentryAction::kWildCard, &wildcard));
  SentryAction all(SentryAction::Action::ALL);
  ASSERT_TRUE(all.Implies(wildcard));
  ASSERT_TRUE(wildcard.Implies(all));

  // Unsupported action, such as '+', throws invalid argument error.
  SentryAction invalid_action;
  Status s = SentryAction::FromString("+", &invalid_action);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

} // namespace sentry
} // namespace kudu
