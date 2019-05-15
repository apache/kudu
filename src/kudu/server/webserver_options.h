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
#ifndef KUDU_SERVER_WEBSERVER_OPTIONS_H
#define KUDU_SERVER_WEBSERVER_OPTIONS_H

#include <cstdint>
#include <functional>
#include <string>

namespace kudu {

// Options controlling the web server.
// The default constructor sets these from the gflags defined in webserver_options.cc.
// See those flags for documentation.
struct WebserverOptions {
  WebserverOptions();

  std::string bind_interface;
  std::string webserver_advertised_addresses;
  uint16_t port;
  std::string doc_root;
  bool enable_doc_root;
  std::string certificate_file;
  std::string private_key_file;
  std::string private_key_password_cmd;
  std::string authentication_domain;
  std::string password_file;
  std::string tls_ciphers;
  std::string tls_min_protocol;
  uint32_t num_worker_threads;

  bool require_spnego;

  // When a user has authenticated via SPNEGO, the SPN of that user will be passed to this
  // callback. This is currently used only for testing.
  std::function<void(const std::string&)> spnego_post_authn_callback;
};

} // namespace kudu
#endif /* KUDU_SERVER_WEBSERVER_OPTIONS_H */
