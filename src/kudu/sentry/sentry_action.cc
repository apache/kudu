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

#include <boost/algorithm/string/predicate.hpp>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"

using std::string;
using strings::Substitute;

namespace kudu {
namespace sentry {

const char* const SentryAction::kWildCard = "*";

SentryAction::SentryAction()
  : action_(Action::UNINITIALIZED) {
}

SentryAction::SentryAction(Action action)
  : action_(action) {
}

Status SentryAction::FromString(const string& action) {
  // Consider action '*' equals to ALL to be compatible with the existing
  // Java Sentry client.
  //
  // See org.apache.sentry.api.service.thrift.SentryPolicyServiceClientDefaultImpl.
  if (boost::iequals(action, "ALL") || action == kWildCard) {
    action_ = Action::ALL;
  } else if (boost::iequals(action, "METADATA")) {
    action_ = Action::METADATA;
  } else if (boost::iequals(action, "SELECT")) {
    action_ = Action::SELECT;
  } else if (boost::iequals(action, "INSERT")) {
    action_ = Action::INSERT;
  } else if (boost::iequals(action, "UPDATE")) {
    action_ = Action::UPDATE;
  } else if (boost::iequals(action, "DELETE")) {
    action_ = Action::DELETE;
  } else if (boost::iequals(action, "ALTER")) {
    action_ = Action::ALTER;
  } else if (boost::iequals(action, "CREATE")) {
    action_ = Action::CREATE;
  } else if (boost::iequals(action, "DROP")) {
    action_ = Action::DROP;
  } else if (boost::iequals(action, "OWNER")) {
    action_ = Action::OWNER;
  } else {
    return Status::InvalidArgument(Substitute("unknown SentryAction: $0",
                                              action));
  }

  return Status::OK();
}

bool SentryAction::Imply(const SentryAction& other) const {
  // Any action must be initialized.
  CHECK(action() != Action::UNINITIALIZED);
  CHECK(other.action() != Action::UNINITIALIZED);

  // Action ALL and OWNER subsume every other action.
  if (action() == Action::ALL ||
      action() == Action::OWNER) {
    return true;
  }

  // Any action subsumes Action METADATA
  if (other.action() == Action::METADATA) {
    return true;
  }

  return action() == other.action();
}

} // namespace sentry
} // namespace kudu
