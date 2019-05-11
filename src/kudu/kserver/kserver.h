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

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/server/server_base.h"
#include "kudu/util/threadpool.h"

namespace kudu {
class Status;

namespace server {
struct ServerBaseOptions;
}

namespace kserver {

// Kudu server instance.
//
// While ServerBase is a generic C++ "server" class, KuduServer includes
// functionality that is specific to Kudu (i.e. pertains to the hosting of
// tablets, to Raft consensus, etc.).
class KuduServer : public server::ServerBase {
 public:
  // Constructs a new KuduServer instance and performs all no-fail member
  // initializations.
  KuduServer(std::string name,
             const server::ServerBaseOptions& options,
             const std::string& metric_namespace);

  // Finalizes the initialization of a KuduServer by performing any member
  // initializations that may fail.
  virtual Status Init() override;

  // Exposes an initialized KuduServer to the network or filesystem.
  virtual Status Start() override;

  // Shuts down a KuduServer instance.
  virtual void Shutdown() override;

#ifdef FB_DO_NOT_REMOVE
  ThreadPool* tablet_prepare_pool() const { return tablet_prepare_pool_.get(); }
  ThreadPool* tablet_apply_pool() const { return tablet_apply_pool_.get(); }
#endif

  ThreadPool* raft_pool() const { return raft_pool_.get(); }

 private:

#ifdef FB_DO_NOT_REMOVE
  // Thread pool for preparing transactions, shared between all tablets.
  gscoped_ptr<ThreadPool> tablet_prepare_pool_;

  // Thread pool for applying transactions, shared between all tablets.
  gscoped_ptr<ThreadPool> tablet_apply_pool_;
#endif

  // Thread pool for Raft-related operations, shared between all tablets.
  gscoped_ptr<ThreadPool> raft_pool_;

  DISALLOW_COPY_AND_ASSIGN(KuduServer);
};

} // namespace kserver
} // namespace kudu
