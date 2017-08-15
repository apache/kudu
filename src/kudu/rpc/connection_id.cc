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

#include "kudu/rpc/connection_id.h"

#include <boost/functional/hash.hpp>

#include "kudu/gutil/strings/substitute.h"

using std::string;

namespace kudu {
namespace rpc {

ConnectionId::ConnectionId() {}

ConnectionId::ConnectionId(const Sockaddr& remote, UserCredentials user_credentials) {
  remote_ = remote;
  user_credentials_ = std::move(user_credentials);
}

void ConnectionId::set_user_credentials(UserCredentials user_credentials) {
  user_credentials_ = std::move(user_credentials);
}

string ConnectionId::ToString() const {
  return strings::Substitute("{remote=$0, user_credentials=$1}",
                             remote_.ToString(),
                             user_credentials_.ToString());
}

size_t ConnectionId::HashCode() const {
  size_t seed = 0;
  boost::hash_combine(seed, remote_.HashCode());
  boost::hash_combine(seed, user_credentials_.HashCode());
  return seed;
}

bool ConnectionId::Equals(const ConnectionId& other) const {
  return (remote() == other.remote()
       && user_credentials().Equals(other.user_credentials()));
}

size_t ConnectionIdHash::operator() (const ConnectionId& conn_id) const {
  return conn_id.HashCode();
}

bool ConnectionIdEqual::operator() (const ConnectionId& cid1, const ConnectionId& cid2) const {
  return cid1.Equals(cid2);
}

} // namespace rpc
} // namespace kudu
