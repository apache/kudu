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

#include <algorithm>
#include <array>
#include <glog/stl_logging.h>
#include <mutex> // for unique_lock
#include <string>
#include <utility>
#include <vector>

#include "kudu/gutil/walltime.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/service_if.h"
#include "kudu/util/atomic.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/trace.h"


DEFINE_bool(rpc_dump_all_traces, false,
            "If true, dump all RPC traces at INFO level");
TAG_FLAG(rpc_dump_all_traces, advanced);
TAG_FLAG(rpc_dump_all_traces, runtime);

using std::pair;
using std::string;
using std::vector;
using std::unique_ptr;

namespace kudu {
namespace rpc {

// Sample an RPC call once every N milliseconds within each
// bucket. If the current sample in a latency bucket is older
// than this threshold, a new sample will be taken.
static const int kSampleIntervalMs = 1000;

static const int kBucketThresholdsMs[] = {10, 100, 1000};
static constexpr int kNumBuckets = arraysize(kBucketThresholdsMs) + 1;

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

    AtomicInt<int64_t> last_sample_time;
    simple_spinlock sample_lock;
    Sample sample;
  };
  std::array<SampleBucket, kNumBuckets> buckets_;

  DISALLOW_COPY_AND_ASSIGN(MethodSampler);
};

MethodSampler* RpczStore::SamplerForCall(InboundCall* call) {
  if (PREDICT_FALSE(!call->method_info())) {
    return nullptr;
  }

  // Most likely, we already have a sampler created for the call.
  {
    shared_lock<rw_spinlock> l(samplers_lock_.get_lock());
    auto it = method_samplers_.find(call->method_info());
    if (PREDICT_TRUE(it != method_samplers_.end())) {
      return it->second.get();
    }
  }

  // If missing, create a new sampler for this method and try to insert it.
  unique_ptr<MethodSampler> ms(new MethodSampler());
  std::lock_guard<percpu_rwlock> lock(samplers_lock_);
  auto it = method_samplers_.find(call->method_info());
  if (it != method_samplers_.end()) {
    return it->second.get();
  }
  auto* ret = ms.get();
  method_samplers_[call->method_info()] = std::move(ms);
  return ret;
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
  int64_t us_since_trace = now - bucket->last_sample_time.Load();
  if (us_since_trace > kSampleIntervalMs * 1000) {
    Sample new_sample = {call->header(), call->trace(), duration_ms};
    {
      std::unique_lock<simple_spinlock> lock(bucket->sample_lock, std::try_to_lock);
      // If another thread is already taking a sample, it's not worth waiting.
      if (!lock.owns_lock()) {
        return;
      }
      std::swap(bucket->sample, new_sample);
      bucket->last_sample_time.Store(now);
    }
    VLOG(1) << "Sampled call " << call->ToString();
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
    if (bucket.last_sample_time.Load() == 0) continue;

    std::unique_lock<simple_spinlock> lock(bucket.sample_lock);
    auto* sample_pb = method_pb->add_samples();
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
  if (PREDICT_FALSE(!sampler)) return;

  sampler->SampleCall(call);
}

void RpczStore::DumpPB(const DumpRpczStoreRequestPB& req,
                       DumpRpczStoreResponsePB* resp) {
  vector<pair<RpcMethodInfo*, MethodSampler*>> samplers;
  {
    shared_lock<rw_spinlock> l(samplers_lock_.get_lock());
    for (const auto& p : method_samplers_) {
      samplers.emplace_back(p.first, p.second.get());
    }
  }

  for (const auto& p : samplers) {
    auto* sampler = p.second;

    RpczMethodPB* method_pb = resp->add_methods();
    // TODO: use the actual RPC name instead of the request type name.
    // Currently this isn't conveniently plumbed here, but the type name
    // is close enough.
    method_pb->set_method_name(p.first->req_prototype->GetTypeName());
    sampler->GetSamplePBs(method_pb);
  }
}

void RpczStore::LogTrace(InboundCall* call) {
  int duration_ms = call->timing().TotalDuration().ToMilliseconds();

  if (call->header_.has_timeout_millis() && call->header_.timeout_millis() > 0) {
    double log_threshold = call->header_.timeout_millis() * 0.75f;
    if (duration_ms > log_threshold) {
      // TODO: consider pushing this onto another thread since it may be slow.
      // The traces may also be too large to fit in a log message.
      LOG(WARNING) << call->ToString() << " took " << duration_ms << "ms (client timeout "
                   << call->header_.timeout_millis() << ").";
      string s = call->trace()->DumpToString();
      if (!s.empty()) {
        LOG(WARNING) << "Trace:\n" << s;
      }
      return;
    }
  }

  if (PREDICT_FALSE(FLAGS_rpc_dump_all_traces)) {
    LOG(INFO) << call->ToString() << " took " << duration_ms << "ms. Trace:";
    call->trace()->Dump(&LOG(INFO), true);
  } else if (duration_ms > 1000) {
    LOG(INFO) << call->ToString() << " took " << duration_ms << "ms. "
              << "Request Metrics: " << call->trace()->MetricsAsJSON();
  }
}


} // namespace rpc
} // namespace kudu
