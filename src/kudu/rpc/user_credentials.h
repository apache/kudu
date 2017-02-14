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

#include "kudu/gutil/macros.h"

namespace kudu {
namespace rpc {

// Client-side user credentials, such as a user's username & password.
// In the future, we will add Kerberos credentials.
//
// TODO(mpercy): this is actually used server side too -- should
// we instead introduce a RemoteUser class or something?
class UserCredentials {
 public:
   UserCredentials();

  // Real user.
  bool has_real_user() const;
  void set_real_user(const std::string& real_user);
  const std::string& real_user() const { return real_user_; }

  // Copy state from another object to this one.
  void CopyFrom(const UserCredentials& other);

  // Returns a string representation of the object, not including the password field.
  std::string ToString() const;

  std::size_t HashCode() const;
  bool Equals(const UserCredentials& other) const;

 private:
  // Remember to update HashCode() and Equals() when new fields are added.
  std::string real_user_;

  DISALLOW_COPY_AND_ASSIGN(UserCredentials);
};

} // namespace rpc
} // namespace kudu
