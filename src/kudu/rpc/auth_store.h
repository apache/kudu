// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.

#ifndef KUDU_RPC_AUTH_STORE_H
#define KUDU_RPC_AUTH_STORE_H

#include <string>
#include <tr1/unordered_map>

#include "kudu/gutil/macros.h"

namespace kudu {

class Status;

namespace rpc {

using std::string;
using std::tr1::unordered_map;

// This class stores username / password pairs in memory for use in PLAIN SASL auth.
// Add() is NOT thread safe.
// Authenticate() is safe to call from multiple threads.
class AuthStore {
 public:
  AuthStore();
  virtual ~AuthStore();

  // Add user to the auth store.
  virtual Status Add(const string& user, const string& password);

  // Validate whether user/password combination exists in auth store.
  // Returns OK if the user has valid credentials.
  // Returns NotFound if the user is not found.
  // Returns NotAuthorized if the password is incorrect.
  virtual Status Authenticate(const string& user, const string& password) const;

 private:
  unordered_map<string, string> user_cred_map_;

  DISALLOW_COPY_AND_ASSIGN(AuthStore);
};

// This class simply allows anybody through.
class DummyAuthStore : public AuthStore {
 public:
  DummyAuthStore();
  virtual ~DummyAuthStore();

  // Always returns OK
  virtual Status Authenticate(const string& user, const string& password) const OVERRIDE;
};

} // namespace rpc
} // namespace kudu

#endif // KUDU_RPC_AUTH_STORE_H
