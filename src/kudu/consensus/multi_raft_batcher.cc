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

#include "kudu/consensus/multi_raft_batcher.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex> // IWYU pragma: keep
#include <ostream>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/multi_raft_consensus_data.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/rpc/periodic.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {
class DnsResolver;

namespace rpc {
class Messenger;
}  // namespace rpc
}  // namespace kudu

DECLARE_int32(consensus_rpc_timeout_ms);
DECLARE_int32(raft_heartbeat_interval_ms);

DEFINE_int32(multi_raft_heartbeat_window_ms, 100,
  "The batch time window for heartbeat batching. "
  "For minimal delay and still maximum effectiveness set "
  "multi_raft_heartbeat_window_ms = raft_heartbeat_interval_ms * ("
  "multi_raft_batch_size / {estimated follower peers from the same host})"
  " + {a little tolerance}; "
  "however, a 0.1 * raft_heartbeat_interval_ms is good, and there is no reason to "
  "set it any lower. "
  "This value is also forced to be less than or equal to half of "
  "raft_heartbeat_interval_ms, because it makes no sense to introduce a possible "
  "delay comparable to the heartbeat interval."
);
TAG_FLAG(multi_raft_heartbeat_window_ms, experimental);

DEFINE_bool(enable_multi_raft_heartbeat_batcher,
            false,
            "Whether to enable the batching of raft heartbeats.");
TAG_FLAG(enable_multi_raft_heartbeat_batcher, experimental);

DEFINE_int32(multi_raft_batch_size, 30, "Maximum batch size for a multi-raft consensus payload.");
TAG_FLAG(multi_raft_batch_size, experimental);
DEFINE_validator(multi_raft_batch_size,
                 [](const char* /*flagname*/, int32_t value) { return value > 0; });

namespace kudu {
namespace consensus {

using kudu::DnsResolver;
using rpc::PeriodicTimer;

uint64_t MultiRaftHeartbeatBatcher::Subscribe(const PeriodicHeartbeater& heartbeater) {
  DCHECK(!closed_);
  std::lock_guard l(heartbeater_lock_);
  auto it = peers_.insert({next_id_, heartbeater});
  DCHECK(it.second) << "Peer with id " << next_id_ << " already exists";
  queue_.push_back(
      {MonoTime::Now() + MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms), next_id_});
  return next_id_++;
}

void MultiRaftHeartbeatBatcher::Unsubscribe(uint64_t id) {
  std::lock_guard l(heartbeater_lock_);
  DCHECK_EQ(1, peers_.count(id));
  peers_.erase(id);
}

MultiRaftHeartbeatBatcher::MultiRaftHeartbeatBatcher(
    const kudu::HostPort& hostport,
    DnsResolver* dns_resolver,
    std::shared_ptr<kudu::rpc::Messenger> messenger,
    MonoDelta flush_interval,
    std::shared_ptr<ThreadPoolToken> raft_pool_token)
    : messenger_(std::move(messenger)),
      consensus_proxy_(std::make_unique<ConsensusServiceProxy>(messenger_, hostport, dns_resolver)),
      batch_time_window_(flush_interval),
      raft_pool_token_(std::move(raft_pool_token)),
      closed_(false) {}

void MultiRaftHeartbeatBatcher::StartTimer() {
  std::weak_ptr<MultiRaftHeartbeatBatcher> const weak_peer = shared_from_this();
  DCHECK(!heartbeat_timer_) << "Heartbeat timer started twice";
  heartbeat_timer_ = PeriodicTimer::Create(
      messenger_,
      [weak_peer]() {
        if (auto peer = weak_peer.lock()) {
          peer->PrepareNextBatch();
        }
      },
      batch_time_window_);
  heartbeat_timer_->Start();
}

void MultiRaftHeartbeatBatcher::PrepareNextBatch() {
  std::vector<PeriodicHeartbeater> current_calls;
  current_calls.reserve(FLAGS_multi_raft_batch_size);

  auto submit_callbacks = [&]() {
    auto this_ptr = shared_from_this();
    KUDU_WARN_NOT_OK(
        raft_pool_token_->Submit([this_ptr, current_calls = std::move(current_calls)]() {
          this_ptr->SendOutScheduled(current_calls);
        }),
        "Failed to submit multi-raft heartbeat batcher task");
    current_calls.clear();
    current_calls.reserve(FLAGS_multi_raft_batch_size);
  };

  const auto sending_period = MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms);

  std::lock_guard lock(heartbeater_lock_);

  auto send_until = MonoTime::Now() + batch_time_window_;
  if (closed_) {
    return;  // Batcher is closed, raft_pool_token_ might be invalid.
  }

  while (!queue_.empty() && queue_.front().time <= send_until) {
    auto front = queue_.front();
    queue_.pop_front();
    auto peer = FindOrNull(peers_, front.id);
    if (peer == nullptr) {
      continue;  // Peer was unsubscribed.
    }
    // TODO(martonka): MonoTime::Now() + sending_period would make much more sense,
    // but this is consistent with the old timer behavior.
    auto next_time = front.time + sending_period;
    queue_.push_back({next_time, front.id});
    current_calls.emplace_back(*peer);
    if (current_calls.size() >= FLAGS_multi_raft_batch_size) {
      submit_callbacks();
    }
  }
  if (!current_calls.empty()) {
    submit_callbacks();
  }
}

void MultiRaftHeartbeatBatcher::MultiRaftUpdateHeartbeatResponseCallback(
    std::shared_ptr<MultiRaftConsensusData> data) {
  for (int i = 0; i < data->batch_req.consensus_requests_size(); i++) {
    const auto* resp = data->batch_res.consensus_responses_size() > i
                           ? &data->batch_res.consensus_responses(i)
                           : nullptr;
    data->response_callback_data[i](data->controller, data->batch_res, resp);
  }
}

void MultiRaftHeartbeatBatcher::Shutdown() {
  {
    std::lock_guard lock(heartbeater_lock_);
    if (closed_) {
      return;  // Already closed.
    }
    closed_ = true;
  }
  if (heartbeat_timer_) {
    heartbeat_timer_->Stop();
    heartbeat_timer_.reset();
  }
}

void MultiRaftHeartbeatBatcher::SendOutScheduled(
    const std::vector<PeriodicHeartbeater>& scheduled_callbacks) {
  // No need to hold the lock here, as we are not modifying the state of the batcher.
  DCHECK_LT(0, scheduled_callbacks.size());
  DCHECK_GE(FLAGS_multi_raft_batch_size, scheduled_callbacks.size());
  auto data = std::make_shared<MultiRaftConsensusData>(scheduled_callbacks.size());

  for (const auto& cb : scheduled_callbacks) {
    cb(data.get());
  }
  // If all called heartbeaters already have another call in progress, or pending elements
  // in its queue, we might end up with an empty batch.
  if (data->batch_req.consensus_requests_size() == 0) {
    return;
  }

  DCHECK(data->batch_req.IsInitialized());
  VLOG(1) << "Sending BatchRequest with size: " << data->batch_req.consensus_requests_size();

  data->controller.set_timeout(MonoDelta::FromMilliseconds(FLAGS_consensus_rpc_timeout_ms));

  // Copy data shared pointer to ensure that it remains valid
  consensus_proxy_->MultiRaftUpdateConsensusAsync(
      data->batch_req, &data->batch_res, &data->controller, [data, inst = shared_from_this()]() {
        inst->MultiRaftUpdateHeartbeatResponseCallback(data);
      });
}

namespace {
MonoDelta GetTimeWindow() {
  // We don't want to delay more than half of the raft heartbeat interval.
  // Even a quarter of the interval starts to be questionable delay,
  // but half is for sure too much.
  auto flush_interval =
      std::min(FLAGS_multi_raft_heartbeat_window_ms, FLAGS_raft_heartbeat_interval_ms / 2);
  if (flush_interval != FLAGS_multi_raft_heartbeat_window_ms) {
    LOG(ERROR) << "multi_raft_heartbeat_window_ms should not be more than "
               << " raft_heartbeat_interval_ms / 2. , forcing multi_raft_heartbeat_window_ms = "
               << flush_interval;
  }
  return MonoDelta::FromMilliseconds(flush_interval);
}
}  // namespace

MultiRaftManager::MultiRaftManager(kudu::DnsResolver* dns_resolver,
                                   const scoped_refptr<MetricEntity>& entity)
  : dns_resolver_(dns_resolver),
    batch_time_window_(GetTimeWindow()),
    closed_(false)
{}

void MultiRaftManager::Init(const std::shared_ptr<rpc::Messenger>& messenger,
                            ThreadPool* raft_pool) {
  DCHECK(!closed_);
  messenger_ = messenger;
  raft_pool_ = raft_pool;
}

MultiRaftHeartbeatBatcherPtr MultiRaftManager::AddOrGetBatcher(
    const kudu::consensus::RaftPeerPB& remote_peer_pb) {
  if (!FLAGS_enable_multi_raft_heartbeat_batcher) {
    return nullptr;  // Batching is disabled.
  }

  DCHECK(messenger_);
  DCHECK(raft_pool_);
  auto hostport = HostPortFromPB(remote_peer_pb.last_known_addr());
  std::lock_guard lock(mutex_);
  if (closed_) {
    DCHECK(false) << "We are in shutdown phase but still adding a peer. Should never happen";
    return nullptr;
  }
  MultiRaftHeartbeatBatcherPtr batcher;

  // After taking the lock, check if there is already a batcher
  // for the same remote host and return it.
  // If we used to have replicas shared with this host, but they were all removed,
  // the old batcher might be deleted already (so the weak ptr is expired).
  auto res = FindOrNull(batchers_, hostport);
  if (res && (batcher = res->batcher.lock())) {
    return batcher;
  }
  // We still have the token for this host, even if the batcher was deleted.
  auto raft_pool_token = res ? res->raft_pool_token
                             : std::shared_ptr<ThreadPoolToken>(move(
                                   raft_pool_->NewToken(ThreadPool::ExecutionMode::CONCURRENT)));
  batcher = std::make_shared<MultiRaftHeartbeatBatcher>(
      hostport, dns_resolver_, messenger_, batch_time_window_, raft_pool_token);
  batcher->StartTimer();
  batchers_.insert_or_assign(hostport, BatcherAndPoolToken(batcher, raft_pool_token));
  return batcher;
}

void MultiRaftManager::Shutdown() {
  std::unordered_map<HostPort, BatcherAndPoolToken, HostPortHasher> batchers;
  {
    std::lock_guard lock(mutex_);
    batchers.swap(batchers_);
  }
  for (auto& entry : batchers) {
    if (auto batcher = entry.second.batcher.lock()) {
      batcher->Shutdown();
    }
    entry.second.raft_pool_token->Shutdown();
  }
  batchers_.clear();
}

}  // namespace consensus
}  // namespace kudu
