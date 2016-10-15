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

#include <set>
#include <string>

namespace kudu {
class Status;

namespace security {

// Represent a very simple access control list which contains a set of users.
//
// This is basically just a wrapper around a set<string> with a bit of parsing logic and
// support for the '*' wildcard.
class SimpleAcl {
 public:
  SimpleAcl();
  ~SimpleAcl();

  // Parse a flag value, which should be of the form 'user1,user2,user3' to indicate a
  // list of users, or '*' to indicate a wildcard. This syntax may be expanded later to
  // include groups, "allow/deny" style access, etc.
  //
  // Thread-unsafe: must be called before the ACL may be consulted, and may not be
  // called a second time concurrent with reads of the ACL.
  Status ParseFlag(const std::string& flag);

  // Return true if the given user is allowed by the ACL.
  //
  // Thread-safe after initialization.
  bool UserAllowed(const std::string& username);

  // Reset the ACL to the specific set of usernames.
  void Reset(std::set<std::string> users);

 private:
  // The set of users, or a set with the single value '*' for the wildcard.
  std::set<std::string> users_;
};

} // namespace security
} // namespace kudu
