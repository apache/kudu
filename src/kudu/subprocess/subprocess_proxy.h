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

#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <google/protobuf/any.pb.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

namespace kudu {

class Env;
class MetricEntity;

namespace subprocess {

// Return a string that can serve as the contents of a log4j2 properties file,
// with the given logging parameters.
std::string Log4j2Properties(const std::string& creator, const std::string& log_dir,
                             const std::string& log_filename, int rollover_size_mb,
                             int max_files, const std::string& log_level,
                             bool log_to_stdout);

// Template that wraps a SubprocessServer, exposing only the underlying ReqPB
// and RespPB as an interface. The given MetricsPB will be initialized,
// allowing for metrics specific to each specialized SubprocessServer.
template<class ReqPB, class RespPB, class MetricsPB>
class SubprocessProxy {
 public:
  SubprocessProxy(Env* env, const std::string& receiver_file,
                  std::vector<std::string> argv, const scoped_refptr<MetricEntity>& entity)
      : server_(new SubprocessServer(env, receiver_file, std::move(argv), MetricsPB(entity))) {}

  // Starts the underlying subprocess.
  Status Start() {
    return server_->Init();
  }

  // Executes the given request and populates the given response, returning a
  // non-OK Status if there was an error sending the request (e.g. timed out)
  // or if there was an error in the response.
  Status Execute(const ReqPB& req, RespPB* resp) {
    SubprocessRequestPB sreq;
    sreq.mutable_request()->PackFrom(req);
    SubprocessResponsePB sresp;
    RETURN_NOT_OK(server_->Execute(&sreq, &sresp));
    if (!sresp.response().UnpackTo(resp)) {
      LOG(ERROR) << strings::Substitute("unable to unpack response: $0",
                                        pb_util::SecureDebugString(sresp));
      return Status::Corruption("unable to unpack response");
    }
    if (sresp.has_error()) {
      return StatusFromPB(sresp.error());
    }
    return Status::OK();
  }

  // Replaces the subprocess server.
  void ReplaceServerForTests(std::unique_ptr<SubprocessServer> server) {
    server_ = std::move(server);
  }
 private:
  std::unique_ptr<SubprocessServer> server_;
};

} // namespace subprocess
} // namespace kudu
