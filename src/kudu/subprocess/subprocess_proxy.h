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

#include <vector>
#include <string>

#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

namespace kudu {
namespace subprocess {

// TODO(awong): add server metrics.
struct SubprocessMetrics {
  scoped_refptr<Histogram> inbound_queue_length;
  scoped_refptr<Histogram> outbound_queue_length;
  scoped_refptr<Histogram> inbound_queue_time_ms;
  scoped_refptr<Histogram> outbound_queue_time_ms;
  scoped_refptr<Histogram> execution_time_ms;
};

// Template that wraps a SubprocessServer, exposing only the underlying ReqPB
// and RespPB as an interface. The given MetricsPB will be initialized,
// allowing for metrics specific to each specialized SubprocessServer.
template<class ReqPB, class RespPB, class MetricsPB>
class SubprocessProxy {
 public:
  SubprocessProxy(std::vector<std::string> argv, const scoped_refptr<MetricEntity>& entity)
      : server_(std::move(argv)), metrics_(entity) {}

  // Starts the underlying subprocess.
  Status Start() {
    return server_.Init();
  }

  // Executes the given request and populates the given response, returning a
  // non-OK Status if there was an error sending the request (e.g. timed out)
  // or if there was an error in the response.
  Status Execute(const ReqPB& req, RespPB* resp) {
    SubprocessRequestPB sreq;
    sreq.mutable_request()->PackFrom(req);
    SubprocessResponsePB sresp;
    RETURN_NOT_OK(server_.Execute(&sreq, &sresp));
    if (!sresp.response().UnpackTo(resp)) {
      LOG(ERROR) << strings::Substitute("unable to unpack response: $0",
                                        pb_util::SecureDebugString(sresp));
      return Status::Corruption("unable to unpack response");
    }
    // The subprocess metrics should still be valid regardless of whether there
    // was an error, so parse them first.
    if (sresp.has_metrics()) {
      ParseMetricsPB(sresp.metrics());
    }
    if (sresp.has_error()) {
      return StatusFromPB(sresp.error());
    }
    return Status::OK();
  }
 private:
  // Parses the given metrics protobuf and updates 'metrics_' based on its
  // contents.
  void ParseMetricsPB(const SubprocessMetricsPB& pb) {
    DCHECK(pb.has_inbound_queue_length());
    DCHECK(pb.has_outbound_queue_length());
    DCHECK(pb.has_inbound_queue_time_ms());
    DCHECK(pb.has_outbound_queue_time_ms());
    DCHECK(pb.has_execution_time_ms());
    metrics_.inbound_queue_length->Increment(pb.inbound_queue_length());
    metrics_.outbound_queue_length->Increment(pb.outbound_queue_length());
    metrics_.inbound_queue_time_ms->Increment(pb.inbound_queue_time_ms());
    metrics_.outbound_queue_time_ms->Increment(pb.outbound_queue_time_ms());
    metrics_.execution_time_ms->Increment(pb.execution_time_ms());
  }

  SubprocessServer server_;
  MetricsPB metrics_;
};

} // namespace subprocess
} // namespace kudu
