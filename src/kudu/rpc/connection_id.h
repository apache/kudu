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

#include "kudu/rpc/user_credentials.h"
#include "kudu/util/net/sockaddr.h"

namespace kudu {
namespace rpc {

// Used to key on Connection information.
// For use as a key in an unordered STL collection, use ConnectionIdHash and ConnectionIdEqual.
// This class is copyable for STL compatibility, but not assignable (use CopyFrom() for that).
class ConnectionId {
 public:
  ConnectionId() {}

  // Copy constructor required for use with STL unordered_map.
  ConnectionId(const ConnectionId& other) = default;

  // Convenience constructor.
  ConnectionId(const Sockaddr& remote,
               std::string hostname,
               UserCredentials user_credentials);

  // The remote address.
  void set_remote(const Sockaddr& remote);
  const Sockaddr& remote() const { return remote_; }

  const std::string& hostname() const { return hostname_; }

  // The credentials of the user associated with this connection, if any.
  void set_user_credentials(UserCredentials user_credentials);
  const UserCredentials& user_credentials() const { return user_credentials_; }
  UserCredentials* mutable_user_credentials() { return &user_credentials_; }

  // Returns a string representation of the object, not including the password field.
  std::string ToString() const;

  size_t HashCode() const;
  bool Equals(const ConnectionId& other) const;

 private:
  // Remember to update HashCode() and Equals() when new fields are added.
  Sockaddr remote_;

  // The original host name before it was resolved to 'remote_'.
  // This must be retained since it is used to compute Kerberos Service Principal Names (SPNs).
  std::string hostname_;

  UserCredentials user_credentials_;
};

class ConnectionIdHash {
 public:
  std::size_t operator() (const ConnectionId& conn_id) const;
};

class ConnectionIdEqual {
 public:
  bool operator() (const ConnectionId& cid1, const ConnectionId& cid2) const;
};

} // namespace rpc
} // namespace kudu
