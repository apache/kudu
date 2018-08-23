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

#include <cstdint>
#include <memory>
#include <string>

#include "kudu/gutil/port.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace kudu {

class Subprocess;

namespace sentry {

class MiniSentry {
 public:

  MiniSentry();

  ~MiniSentry();

  // Starts the mini Sentry service.
  //
  // If the MiniSentry has already been started and stopped, it will be restarted
  // using the same listening port.
  Status Start() WARN_UNUSED_RESULT;

  // Stops the mini Sentry service.
  Status Stop() WARN_UNUSED_RESULT;

  // Pause the Sentry service.
  Status Pause() WARN_UNUSED_RESULT;

  // Unpause the Sentry service.
  Status Resume() WARN_UNUSED_RESULT;

  // Returns the address of the Sentry service. Should only be called after the
  // Sentry service is started.
  HostPort address() const {
    return HostPort("127.0.0.1", port_);
  }

 private:

  // Creates a sentry-site.xml for the mini Sentry.
  Status CreateSentrySite(const std::string& tmp_dir) const WARN_UNUSED_RESULT;

  // Waits for the metastore process to bind to a port.
  Status WaitForSentryPorts() WARN_UNUSED_RESULT;

  std::unique_ptr<Subprocess> sentry_process_;
  uint16_t port_ = 0;
};

} // namespace sentry
} // namespace kudu
