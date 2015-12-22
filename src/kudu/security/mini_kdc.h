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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/util/status.h"

namespace kudu {

class Env;
class Subprocess;

struct MiniKdcOptions {

  // Kerberos Realm.
  // Default: "KRBTEST.COM"
  std::string realm;

  // Directory in which to store data.
  // Default: "", which auto-generates a unique path for this KDC.
  // The default may only be used from a gtest unit test.
  std::string data_root;

  // KDC port.
  // Default: 0 (ephemeral port).
  uint16_t port = 0;

  // Returns a string representation of the options suitable for debug printing.
  std::string ToString() const;
};

class MiniKdc {
 public:
  // Creates a new MiniKdc with the default options.
  MiniKdc();

  // Creates a new MiniKdc with the provided options.
  explicit MiniKdc(const MiniKdcOptions& options);

  ~MiniKdc();

  // Starts the mini Kerberos KDC.
  Status Start() WARN_UNUSED_RESULT;

  // Stops the mini Kerberos KDC.
  Status Stop() WARN_UNUSED_RESULT;

  uint16_t port() const {
    CHECK(kdc_process_) << "must start first";
    return options_.port;
  }

  // Creates a new user with the given username.
  // The password is the same as the username.
  Status CreateUserPrincipal(const std::string& username) WARN_UNUSED_RESULT;

  // Creates a new service principal and associated keytab, returning its
  // path in 'path'. 'spn' is the desired service principal name
  // (e.g. "kudu/foo.example.com"). If the principal already exists, its key
  // will be reset and a new keytab will be generated.
  Status CreateServiceKeytab(const std::string& spn, std::string* path);

  // Kinit a user to the mini KDC.
  Status Kinit(const std::string& username) WARN_UNUSED_RESULT;

  // Call the 'klist' utility.  This is useful for logging the local ticket
  // cache state.
  Status Klist(std::string* output) WARN_UNUSED_RESULT;

  // Call the 'klist' utility to list the contents of a specific keytab.
  Status KlistKeytab(const std::string& keytab_path,
                     std::string* output) WARN_UNUSED_RESULT;

  // Sets the environment variables used by the krb5 library
  // in the current process. This points the SASL library at the
  // configuration associated with this KDC.
  Status SetKrb5Environment() const;

 private:
  // Returns a map of the necessary Kerberos environment variables.
  std::map<std::string, std::string> GetEnvVars() const;

  // Prepends required Kerberos environment variables to the process arguments.
  std::vector<std::string> MakeArgv(const std::vector<std::string>& in_argv);

  // Creates a kdc.conf in the data root.
  Status CreateKrb5Conf() const WARN_UNUSED_RESULT;

  // Creates a krb5.conf in the data root.
  Status CreateKdcConf() const WARN_UNUSED_RESULT;

  // Determine the ports that the KDC bound to. Will wait for the KDC if it is
  // still initializing.
  Status WaitForKdcPorts() WARN_UNUSED_RESULT;

  std::unique_ptr<Subprocess> kdc_process_;
  MiniKdcOptions options_;
};

} // namespace kudu
