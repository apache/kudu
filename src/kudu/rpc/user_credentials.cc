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

#include "kudu/rpc/user_credentials.h"

#include <cstddef>
#include <string>
#include <utility>

#include <boost/functional/hash/hash.hpp>

#include "kudu/gutil/strings/substitute.h"

using std::string;

namespace kudu {
namespace rpc {

bool UserCredentials::has_real_user() const {
  return !real_user_.empty();
}

void UserCredentials::set_real_user(string real_user) {
  real_user_ = std::move(real_user);
}

string UserCredentials::ToString() const {
  // Does not print the password.
  return strings::Substitute("{real_user=$0}", real_user_);
}

size_t UserCredentials::HashCode() const {
  size_t seed = 0;
  if (has_real_user()) {
    boost::hash_combine(seed, real_user());
  }
  return seed;
}

bool UserCredentials::Equals(const UserCredentials& other) const {
  return real_user() == other.real_user();
}

} // namespace rpc
} // namespace kudu
