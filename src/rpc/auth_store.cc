// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "rpc/auth_store.h"

#include <string>
#include <tr1/unordered_map>

#include "util/status.h"

namespace kudu {
namespace rpc {

AuthStore::AuthStore() {
}

Status AuthStore::Add(const string& user, const string& pass) {
  user_cred_map_[user] = pass;
  return Status::OK();
}

Status AuthStore::Authenticate(const string& user, const string& pass) const {
  unordered_map<string, string>::const_iterator it = user_cred_map_.find(user);
  if (it == user_cred_map_.end()) {
    return Status::NotFound("Unknown user", user);
  }
  if (it->second != pass) {
    return Status::NotAuthorized("Invalid credentials for user", user);
  }
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
