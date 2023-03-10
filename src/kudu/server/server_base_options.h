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
#ifndef KUDU_SERVER_SERVER_BASE_OPTIONS_H
#define KUDU_SERVER_SERVER_BASE_OPTIONS_H

#include <cstdint>
#include <string>

#include "kudu/fs/fs_manager.h"
#include "kudu/server/webserver_options.h"
#include "kudu/server/rpc_server.h"

namespace kudu {

class Env;

namespace server {

struct ServerKeyInfo {
  std::string server_key;
  std::string server_key_iv;
  std::string server_key_version;

 public:
  ServerKeyInfo();
};

struct TenantKeyInfo {
  std::string tenant_name;
  std::string tenant_id;
  std::string tenant_key;
  std::string tenant_key_iv;
  std::string tenant_key_version;

 public:
  TenantKeyInfo();
};

// Options common to both types of servers.
// The subclass constructor should fill these in with defaults from
// server-specific flags.
struct ServerBaseOptions {
  Env* env;

  FsManagerOpts fs_opts;
  RpcServerOptions rpc_opts;
  WebserverOptions webserver_opts;

  std::string dump_info_path;
  std::string dump_info_format;

  int32_t metrics_log_interval_ms;

  ServerKeyInfo server_key_info;
  TenantKeyInfo tenant_key_info;

 protected:
  ServerBaseOptions();
};

} // namespace server
} // namespace kudu
#endif /* KUDU_SERVER_SERVER_BASE_OPTIONS_H */
