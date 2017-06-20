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

#include "kudu/kserver/kserver.h"

#include <string>
#include <utility>

#include "kudu/server/server_base_options.h"
#include "kudu/util/status.h"

using std::string;

namespace kudu {

using server::ServerBaseOptions;

namespace kserver {

KuduServer::KuduServer(string name,
                       const ServerBaseOptions& options,
                       const string& metric_namespace)
    : ServerBase(std::move(name), options, metric_namespace) {
}

Status KuduServer::Init() {
  RETURN_NOT_OK(ServerBase::Init());
  return Status::OK();
}

Status KuduServer::Start() {
  RETURN_NOT_OK(ServerBase::Start());
  return Status::OK();
}

void KuduServer::Shutdown() {
  ServerBase::Shutdown();
}

} // namespace kserver
} // namespace kudu
