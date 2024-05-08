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

#include <algorithm>  // IWYU pragma: keep
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex> // for unique_lock
#include <ostream>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/message.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/service_if.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/trace.h"
#include "kudu/util/trace_metrics.h"

DEFINE_bool(rpc_dump_all_traces, false,
            "If true, dump all RPC traces at INFO level");
TAG_FLAG(rpc_dump_all_traces, advanced);
TAG_FLAG(rpc_dump_all_traces, runtime);

DEFINE_int32(rpc_duration_too_long_ms, 1000,
             "Threshold (in milliseconds) above which a RPC is considered too long and its "
             "duration and method name are logged at INFO level. The time measured is between "
             "when a RPC is accepted and when its call handler completes.");
TAG_FLAG(rpc_duration_too_long_ms, advanced);
TAG_FLAG(rpc_duration_too_long_ms, runtime);

using std::pair;
using std::shared_lock;
using std::string;
using std::vector;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace rpc {

// Sample an RPC call once every N milliseconds within each
// bucket. If the current sample in a latency bucket is older
// than this threshold, a new sample will be taken.
//
// NOTE: this constant is in microsecond units since corresponding timestamps
//       are captured using the GetMonoTimeMicros() timer.
static constexpr int64_t kSampleIntervalUs = 1000 * 1000;

static constexpr size_t kBucketThresholdsMs[] = {10, 100, 1000};
static constexpr size_t kNumBuckets = arraysize(kBucketThresholdsMs) + 1;

// An instance of this class is created For each RPC method implemented
// on the server. It keeps several recent samples for each RPC, currently
// based on fixed time buckets.
class MethodSampler {
 public:
  MethodSampler() {}
  ~MethodSampler() {}

  // Potentially sample a single call.
  void SampleCall(InboundCall* call);

  // Dump the current samples.
  void GetSamplePBs(RpczMethodPB* pb);

 private:
  // Convert the trace metrics from 't' into protobuf entries in 'sample_pb'.
  // This function recurses through the parent-child relationship graph,
  // keeping the current tree path in 'child_path' (empty at the root).
  static void GetTraceMetrics(const Trace& t,
                              const string& child_path,
                              RpczSamplePB* sample_pb);

  // An individual recorded sample.
  struct Sample {
    RequestHeader header;
    scoped_refptr<Trace> trace;
    int duration_ms;
  };

  // A sample, including the particular time at which it was
  // sampled, and a lock protecting it.
  struct SampleBucket {
    SampleBucket() : last_sample_time(0) {}

    std::atomic<int64_t> last_sample_time;
    simple_spinlock sample_lock;
    Sample sample;
  };
  std::array<SampleBucket, kNumBuckets> buckets_;

  DISALLOW_COPY_AND_ASSIGN(MethodSampler);
};

MethodSampler* RpczStore::SamplerForCall(InboundCall* call) {
  auto* method_info = call->method_info();
  if (PREDICT_FALSE(!method_info)) {
    return nullptr;
  }

  // Most likely, we already have a sampler created for the call once received
  // the very first call for a particular method of an RPC interface.
  {
    shared_lock<rw_spinlock> l(samplers_lock_.get_lock());
    auto it = method_samplers_.find(method_info);
    if (PREDICT_TRUE(it != method_samplers_.end())) {
      return it->second.get();
    }
  }

  // If missing, create a new sampler for this method and try to insert it.
  unique_ptr<MethodSampler> ms(new MethodSampler);
  std::lock_guard<percpu_rwlock> lock(samplers_lock_);
  const auto& [it, _] = method_samplers_.try_emplace(method_info, std::move(ms));
  return it->second.get();
}

void MethodSampler::SampleCall(InboundCall* call) {
  // First determine which sample bucket to put this in.
  int duration_ms = call->timing().TotalDuration().ToMilliseconds();

  SampleBucket* bucket = &buckets_[kNumBuckets - 1];
  for (int i = 0 ; i < kNumBuckets - 1; i++) {
    if (duration_ms < kBucketThresholdsMs[i]) {
      bucket = &buckets_[i];
      break;
    }
  }

  MicrosecondsInt64 now = GetMonoTimeMicros();
  int64_t us_since_trace = now - bucket->last_sample_time;
  if (us_since_trace > kSampleIntervalUs) {
    {
      std::unique_lock<simple_spinlock> lock(bucket->sample_lock, std::try_to_lock);
      // If another thread is already taking a sample, it's not worth waiting.
      if (!lock.owns_lock()) {
        return;
      }
      bucket->sample = {call->header(), call->trace(), duration_ms};
    }
    bucket->last_sample_time = now;
    VLOG(2) << "Sampled call " << call->ToString();
  }
}

void MethodSampler::GetTraceMetrics(const Trace& t,
                                    const string& child_path,
                                    RpczSamplePB* sample_pb) {
  auto m = t.metrics().Get();
  for (const auto& e : m) {
    auto* pb = sample_pb->add_metrics();
    pb->set_key(e.first);
    pb->set_value(e.second);
    if (!child_path.empty()) {
      pb->set_child_path(child_path);
    }
  }

  for (const auto& child_pair : t.ChildTraces()) {
    string path = child_path;
    if (!path.empty()) {
      path += ".";
    }
    path += child_pair.first.ToString();
    GetTraceMetrics(*child_pair.second.get(), path, sample_pb);
  }
}

void MethodSampler::GetSamplePBs(RpczMethodPB* method_pb) {
  for (auto& bucket : buckets_) {
    if (bucket.last_sample_time == 0) {
      continue;
    }

    auto* sample_pb = method_pb->add_samples();
    std::lock_guard<simple_spinlock> lock(bucket.sample_lock);
    sample_pb->mutable_header()->CopyFrom(bucket.sample.header);
    sample_pb->set_trace(bucket.sample.trace->DumpToString(Trace::INCLUDE_TIME_DELTAS));

    GetTraceMetrics(*bucket.sample.trace.get(), "", sample_pb);
    sample_pb->set_duration_ms(bucket.sample.duration_ms);
  }
}

RpczStore::RpczStore() {}
RpczStore::~RpczStore() {}

void RpczStore::AddCall(InboundCall* call) {
  LogTrace(call);
  auto* sampler = SamplerForCall(call);
  if (PREDICT_FALSE(!sampler)) {
    return;
  }

  sampler->SampleCall(call);
}

void RpczStore::DumpPB(const DumpRpczStoreRequestPB& req,
                       DumpRpczStoreResponsePB* resp) {
  vector<pair<RpcMethodInfo*, MethodSampler*>> samplers;
  {
    shared_lock<rw_spinlock> l(samplers_lock_.get_lock());
    for (const auto& [mi, ms] : method_samplers_) {
      samplers.emplace_back(mi, ms.get());
    }
  }

  for (const auto& [mi, ms] : samplers) {
    RpczMethodPB* method_pb = resp->add_methods();
    // TODO(todd): use the actual RPC name instead of the request type name.
    // Currently this isn't conveniently plumbed here, but the type name
    // is close enough.
    method_pb->set_method_name(mi->req_prototype->GetTypeName());
    ms->GetSamplePBs(method_pb);
  }
}

void RpczStore::LogTrace(InboundCall* call) {
  const auto duration_ms = call->timing().TotalDuration().ToMilliseconds();

  if (call->header_.has_timeout_millis() && call->header_.timeout_millis() > 0) {
    const int64_t timeout_ms = call->header_.timeout_millis();
    const int64_t log_threshold = 3LL * timeout_ms / 4; // 75% of timeout
    if (duration_ms > log_threshold) {
      // This might be slow, but with AsyncLogger it's mostly OK. The only issue
      // is when the logger decides to flush exactly at the point of logging
      // these messages.
      //
      // TODO(aserbin): introduce a hint to the logger to avoid flushing for
      //                a while when logging from a reactor thread or similar,
      //                even if going slightly over the allowed threshold
      //                for the amount of accumulated log messages
      LOG(WARNING) << Substitute("$0 took $1 ms (client timeout $2 ms). Trace:\n",
                                 call->ToString(), duration_ms, timeout_ms);
      call->trace()->Dump(&LOG(WARNING), Trace::INCLUDE_ALL);
      return;
    }
  }
  const bool too_long = duration_ms > FLAGS_rpc_duration_too_long_ms;
  if (too_long || PREDICT_FALSE(FLAGS_rpc_dump_all_traces)) {
    LOG(INFO) << Substitute("$0 took $1 ms. Trace:\n", call->ToString(), duration_ms);
    call->trace()->Dump(&LOG(INFO), too_long ? Trace::INCLUDE_ALL
                                             : Trace::INCLUDE_TIME_DELTAS);
  }
}


} // namespace rpc
} // namespace kudu
