// Copyright (c) 2013, Cloudera, inc.

#include <string>
#include <vector>

#include "consensus/consensus_peers.h"
#include "consensus/consensus_queue.h"
#include "util/countdown_latch.h"
#include "util/locks.h"
#include "util/threadpool.h"

namespace kudu {
namespace consensus {

// An operation status for tests that allows to wait for operations
// to complete.
class TestOperationStatus : public OperationStatus {
 public:
  explicit TestOperationStatus(int n_majority, int total_peers, const OpId& op_id)
    : op_id_(op_id),
      majority_latch_(n_majority),
      all_replicated_latch_(total_peers),
      replicated_count_(0) {
  }

  void AckPeer(const string& uuid) {
    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG(2) << "Peer: " << uuid << " Ack'd op: " << op_id_.ShortDebugString();
    }
    boost::lock_guard<simple_spinlock> lock(lock_);
    replicated_count_++;
    majority_latch_.CountDown();
    all_replicated_latch_.CountDown();
  }

  bool IsDone() const {
    return majority_latch_.count() == 0;
  }

  bool IsAllDone() const {
    return all_replicated_latch_.count() == 0;
  }

  void Wait() {
    majority_latch_.Wait();
  }

  void WaitAllReplicated() {
    all_replicated_latch_.Wait();
  }

  int replicated_count() const {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return replicated_count_;
  }

 private:
  OpId op_id_;
  CountDownLatch majority_latch_;
  CountDownLatch all_replicated_latch_;
  int replicated_count_;
  mutable simple_spinlock lock_;
};

// Appends 'count' messages to 'queue' with different terms and indexes.
//
// An operation will only be considered done (TestOperationStatus::IsDone()
// will become true) once at least 'n_majority' peers have called
// TestOperationStatus::AckPeer().
//
// If the 'statuses_collector' vector is not NULL the operation statuses will
// be added to it.
static inline void AppendReplicateMessagesToQueue(
    PeerMessageQueue* queue,
    int first,
    int count,
    int n_majority = 1,
    int total_peers = 1,
    vector<scoped_refptr<OperationStatus> >* statuses_collector = NULL) {

  for (int i = first; i < first + count; i++) {
    gscoped_ptr<OperationPB> op(new OperationPB);
    OpId* id = op->mutable_id();
    id->set_term(i / 7);
    id->set_index(i % 7);
    ReplicateMsg* msg = op->mutable_replicate();
    msg->set_op_type(NO_OP);
    scoped_refptr<OperationStatus> status(new TestOperationStatus(n_majority, total_peers, *id));
    queue->AppendOperation(op.Pass(), status);
    if (statuses_collector) {
      statuses_collector->push_back(status);
    }
  }
}

// Allows to test peers by emulating a noop remote endpoint that just replies
// that the messages were received/replicated/committed.
class NoOpTestPeerProxy : public PeerProxy {
 public:
  NoOpTestPeerProxy()
    : pool_("noop-peer-pool", 0, 1, ThreadPool::DEFAULT_TIMEOUT),
      delay_response_(false),
      callback_(NULL) {
    CHECK_OK(pool_.Init());
  }

  virtual Status UpdateAsync(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response,
                             rpc::RpcController* controller,
                             const rpc::ResponseCallback& callback) OVERRIDE {
    ConsensusStatusPB* status = response->mutable_status();
    if (request->ops_size() > 0) {
      status->mutable_replicated_watermark()->CopyFrom(
          request->ops(request->ops_size() - 1).id());
      status->mutable_committed_watermark()->CopyFrom(
          request->ops(request->ops_size() - 1).id());
      status->mutable_received_watermark()->CopyFrom(
          request->ops(request->ops_size() - 1).id());
      last_status_.CopyFrom(*status);
    } else if (last_status_.IsInitialized()) {
      status->CopyFrom(last_status_);
    }

    callback_ = &callback;

    if (!delay_response_) {
      RETURN_NOT_OK(Respond());
    }

    return Status::OK();
  }

  const ConsensusStatusPB& last_status() {
    return last_status_;
  }

  // Delays the answer to the next response to this remote
  // peer. The response callback will only be called on Respond().
  void DelayResponse() {
    delay_response_ = true;
  }

  // Answers the peer.
  Status Respond() {
    delay_response_ = false;
    CHECK_NOTNULL(callback_);
    return pool_.SubmitFunc(*callback_);
  }

 private:
  ThreadPool pool_;
  ConsensusStatusPB last_status_;
  bool delay_response_;
  const rpc::ResponseCallback* callback_;
};

class NoOpTestPeerProxyFactory : public PeerProxyFactory {
 public:

  virtual Status NewProxy(const metadata::QuorumPeerPB& peer_pb,
                          gscoped_ptr<PeerProxy>* proxy) OVERRIDE {
    proxy->reset(new NoOpTestPeerProxy());
    return Status::OK();
  }
};

}  // namespace consensus
}  // namespace kudu

