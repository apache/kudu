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

#include "kudu/rpc/rpcz_store.h"

#include <glog/stl_logging.h>
#include <string>

#include "kudu/rpc/inbound_call.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/trace.h"


DEFINE_bool(rpc_dump_all_traces, false,
            "If true, dump all RPC traces at INFO level");
TAG_FLAG(rpc_dump_all_traces, advanced);
TAG_FLAG(rpc_dump_all_traces, runtime);

namespace kudu {
namespace rpc {


RpczStore::RpczStore() {}
RpczStore::~RpczStore() {}

void RpczStore::AddCall(InboundCall* call) {
  LogTrace(call);
}

void RpczStore::LogTrace(InboundCall* call) {
  MonoTime now = MonoTime::Now(MonoTime::FINE);
  int total_time = now.GetDeltaSince(call->timing_.time_received).ToMilliseconds();

  if (call->header_.has_timeout_millis() && call->header_.timeout_millis() > 0) {
    double log_threshold = call->header_.timeout_millis() * 0.75f;
    if (total_time > log_threshold) {
      // TODO: consider pushing this onto another thread since it may be slow.
      // The traces may also be too large to fit in a log message.
      LOG(WARNING) << call->ToString() << " took " << total_time << "ms (client timeout "
                   << call->header_.timeout_millis() << ").";
      std::string s = call->trace()->DumpToString();
      if (!s.empty()) {
        LOG(WARNING) << "Trace:\n" << s;
      }
      return;
    }
  }

  if (PREDICT_FALSE(FLAGS_rpc_dump_all_traces)) {
    LOG(INFO) << call->ToString() << " took " << total_time << "ms. Trace:";
    call->trace()->Dump(&LOG(INFO), true);
  } else if (total_time > 1000) {
    LOG(INFO) << call->ToString() << " took " << total_time << "ms. "
              << "Request Metrics: " << call->trace()->MetricsAsJSON();
  }
}


} // namespace rpc
} // namespace kudu
