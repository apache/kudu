// Copyright (c) 2013, Cloudera, inc.

#include <boost/thread/locks.hpp>
#include <string>
#include <tr1/unordered_map>
#include <tr1/memory>
#include <vector>
#include <utility>

#include "common/wire_protocol.h"
#include "consensus/consensus.h"
#include "consensus/consensus_peers.h"
#include "consensus/consensus_queue.h"
#include "gutil/map-util.h"
#include "gutil/strings/substitute.h"
#include "util/countdown_latch.h"
#include "util/locks.h"
#include "util/threadpool.h"

namespace kudu {
namespace consensus {

using strings::Substitute;

// An operation status for tests that allows to wait for operations
// to complete.
class TestOpStatusTracker : public OperationStatusTracker {
 public:
  TestOpStatusTracker(gscoped_ptr<OperationPB> op, int n_majority, int total_peers)
    : OperationStatusTracker(op.Pass()),
      majority_latch_(n_majority),
      all_replicated_latch_(total_peers),
      replicated_count_(0) {
  }

  void AckPeer(const string& uuid) OVERRIDE {
    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG(2) << "Peer: " << uuid << " Ack'd op: " << operation_->ShortDebugString();
    }
    boost::lock_guard<simple_spinlock> lock(lock_);
    replicated_count_++;
    majority_latch_.CountDown();
    all_replicated_latch_.CountDown();
  }

  bool IsDone() const OVERRIDE {
    return majority_latch_.count() == 0;
  }

  bool IsAllDone() const OVERRIDE {
    return all_replicated_latch_.count() == 0;
  }

  void Wait() OVERRIDE {
    majority_latch_.Wait();
  }

  void WaitAllReplicated() {
    all_replicated_latch_.Wait();
  }

  int replicated_count() const {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return replicated_count_;
  }

  virtual std::string ToString() const OVERRIDE {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return Substitute("Op: $0, IsDone: $1, IsAllDone: $2, ReplicatedCount: $3.",
                      operation_->ShortDebugString(), IsDone(), IsAllDone(), replicated_count_);
  }

 private:
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
    string dummy_payload = "",
    vector<scoped_refptr<OperationStatusTracker> >* statuses_collector = NULL) {

  for (int i = first; i < first + count; i++) {
    gscoped_ptr<OperationPB> op(new OperationPB);
    OpId* id = op->mutable_id();
    id->set_term(i / 7);
    id->set_index(i % 7);
    ReplicateMsg* msg = op->mutable_replicate();
    msg->set_op_type(NO_OP);
    msg->mutable_noop_request()->set_payload_for_tests(dummy_payload);
    scoped_refptr<OperationStatusTracker> status(
        new TestOpStatusTracker(op.Pass(), n_majority, total_peers));
    CHECK_OK(queue->AppendOperation(status));
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
    : delay_response_(false),
      callback_(NULL) {
    CHECK_OK(ThreadPoolBuilder("noop-peer-pool").set_max_threads(1).Build(&pool_));
  }

  virtual Status UpdateAsync(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response,
                             rpc::RpcController* controller,
                             const rpc::ResponseCallback& callback) OVERRIDE {
    ConsensusStatusPB* status = response->mutable_status();
    if (request->ops_size() > 0) {
      status->mutable_replicated_watermark()->CopyFrom(
          request->ops(request->ops_size() - 1).id());
      status->mutable_safe_commit_watermark()->CopyFrom(
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
    return pool_->SubmitFunc(*callback_);
  }

 private:
  gscoped_ptr<ThreadPool> pool_;
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

// Allows to test remote peers by emulating an RPC.
// Both the "remote" peer's RPC call and the caller peer's response are executed
// asynchronously in a ThreadPool.
class LocalTestPeerProxy : public PeerProxy {
 public:
  explicit LocalTestPeerProxy(Consensus* consensus)
    : callback_(NULL),
      consensus_(consensus),
      miss_comm_(false) {
    CHECK_OK(ThreadPoolBuilder("noop-peer-pool").set_max_threads(1).Build(&pool_));
  }

  virtual Status UpdateAsync(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response,
                             rpc::RpcController* controller,
                             const rpc::ResponseCallback& callback) OVERRIDE {
    boost::lock_guard<simple_spinlock> lock(lock_);
    callback_ = &callback;
    RETURN_NOT_OK(pool_->SubmitFunc(boost::bind(&LocalTestPeerProxy::SendToOtherPeer,
                                               this, request, response)));
    return Status::OK();
  }

  void SendToOtherPeer(const ConsensusRequestPB* request,
                       ConsensusResponsePB* response) {
    // Copy the request and the response for the other peer so that ownership
    // remains as close to the dist. impl. as possible.
    ConsensusRequestPB other_peer_req;
    other_peer_req.CopyFrom(*request);
    ConsensusResponsePB other_peer_resp;
    other_peer_resp.CopyFrom(*response);

    Status s;
    {
      boost::lock_guard<simple_spinlock> lock(lock_);
      if (PREDICT_TRUE(consensus_)) {
        s = consensus_->Update(&other_peer_req, &other_peer_resp);
        if (!other_peer_resp.has_error()) {
          CHECK(other_peer_resp.has_status());
          CHECK(other_peer_resp.status().has_replicated_watermark());
          CHECK(other_peer_resp.status().has_safe_commit_watermark());
          CHECK(other_peer_resp.status().has_received_watermark());
        }
      } else {
        s = Status::NotFound("Other consensus instance was destroyed");
      }
    }
    if (!s.ok()) {
      LOG(WARNING) << "Could not update replica. With request: "
          << other_peer_req.ShortDebugString() << " Status: " << s.ToString();
      tserver::TabletServerErrorPB* error = other_peer_resp.mutable_error();
            error->set_code(tserver::TabletServerErrorPB::UNKNOWN_ERROR);
            StatusToPB(s, error->mutable_status());
    }
    response->CopyFrom(other_peer_resp);

    {
      boost::lock_guard<simple_spinlock> lock(lock_);
      if (PREDICT_FALSE(miss_comm_)) {
        tserver::TabletServerErrorPB* error = response->mutable_error();
        error->set_code(tserver::TabletServerErrorPB::UNKNOWN_ERROR);
        StatusToPB(Status::IOError("Artificial error caused by communication failure injection."),
                   error->mutable_status());
        miss_comm_ = false;
      }
      CHECK_OK(pool_->SubmitFunc(*callback_));
    }
  }

  void ConsensusShutdown() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    consensus_ = NULL;
  }

  void InjectCommFaultLeaderSide() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    miss_comm_ = true;
  }

  Consensus* GetTarget() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return consensus_;
  }

 private:
  gscoped_ptr<ThreadPool> pool_;
  const rpc::ResponseCallback* callback_;
  Consensus* consensus_;
  mutable simple_spinlock lock_;
  bool miss_comm_;
};

// For tests, we create local proxies that interact with other RaftConsensus
// instances directly. We need a hook to make sure that, when an instance
// is destroyed, proxies of other instances that point to it no longer
// try to call methods on it, otherwise we get a SIGSEGV.
class UnsetConsensusOnDestroyHook : public Consensus::ConsensusFaultHooks {
 public:
   void AddPeerProxy(LocalTestPeerProxy* proxy) { proxies_.push_back(proxy); }
   virtual Status PreShutdown() OVERRIDE {
     BOOST_FOREACH(LocalTestPeerProxy* proxy, proxies_) {
       proxy->ConsensusShutdown();
     }
     return Status::OK();
   }
 private:
  vector<LocalTestPeerProxy*> proxies_;
};

class LocalTestPeerProxyFactory : public PeerProxyFactory {
 public:
  typedef pair<std::tr1::shared_ptr<UnsetConsensusOnDestroyHook>, Consensus*> Entry;

  // TODO allow to pass other fault hooks
  void AddPeer(const metadata::QuorumPeerPB& peer_pb, Consensus* consensus) {
    std::tr1::shared_ptr<UnsetConsensusOnDestroyHook> hook(new UnsetConsensusOnDestroyHook);
    consensus->SetFaultHooks(hook);
    peers_.insert(pair<std::string, Entry>(peer_pb.permanent_uuid(), Entry(hook, consensus)));
  }

  virtual Status NewProxy(const metadata::QuorumPeerPB& peer_pb,
                          gscoped_ptr<PeerProxy>* proxy) OVERRIDE {
    Entry entry = FindOrDie(peers_, peer_pb.permanent_uuid());
    LocalTestPeerProxy* new_proxy = new LocalTestPeerProxy(entry.second);
    entry.first->AddPeerProxy(new_proxy);
    proxy->reset(new_proxy);
    proxies_.push_back(new_proxy);
    return Status::OK();
  }

  virtual const vector<LocalTestPeerProxy*>& GetProxies() {
    return proxies_;
  }

 private:
  std::tr1::unordered_map<std::string, Entry> peers_;
  // NOTE: There is no need to delete this on the dctor because proxies are externally managed
  vector<LocalTestPeerProxy*> proxies_;
};

class TestTransaction : public ReplicaCommitContinuation {
 public:
  TestTransaction(ThreadPool* pool, gscoped_ptr<ConsensusRound> context)
    : context_(context.Pass()),
      pool_(pool) {
  }

  virtual Status LeaderCommitted(gscoped_ptr<OperationPB> leader_commit_op) OVERRIDE {
    context_->SetLeaderCommitOp(leader_commit_op.Pass());
    RETURN_NOT_OK(pool_->SubmitFunc(boost::bind(&TestTransaction::Commit, this)));
    return Status::OK();
  }

  virtual void Abort() {}

  void Commit() {
    // Normally the replica would have a different commit msg
    // but for tests we just copy the leader's message.
    gscoped_ptr<CommitMsg> msg(new CommitMsg);
    msg->CopyFrom(context_->leader_commit_op()->commit());
    CHECK_OK(context_->Commit(msg.Pass()));
  }

  void Cleanup() {
    delete this;
  }

  void Fatal(const Status& status) {
    LOG(FATAL) << "TestTransaction aborted with status: " << status.ToString();
  }

  gscoped_ptr<ConsensusRound> context_;
 private:
  ThreadPool* pool_;
};

// A transaction factory for tests, usually this is implemented by TabletPeer.
class TestTransactionFactory : public ReplicaTransactionFactory {
 public:
  TestTransactionFactory() {
    CHECK_OK(ThreadPoolBuilder("test-txn-factory").set_max_threads(1).Build(&pool_));
  }

  Status StartReplicaTransaction(gscoped_ptr<ConsensusRound> context) OVERRIDE {
    TestTransaction* txn = new TestTransaction(pool_.get(), context.Pass());
    txn->context_->SetReplicaCommitContinuation(txn);
    std::tr1::shared_ptr<FutureCallback> commit_clbk(
        new BoundFunctionCallback(boost::bind(&TestTransaction::Cleanup, txn),
                                  boost::bind(&TestTransaction::Fatal, txn, _1)));
    txn->context_->SetCommitCallback(commit_clbk);
    return Status::OK();
  }

 void ShutDown() {
   pool_->Shutdown();
 }

 private:
  gscoped_ptr<ThreadPool> pool_;
};

// Consensus fault hooks impl. that simply counts the number of calls to
// each method.
// Allows passing another hook instance so that we can use both.
// If non-null, the passed hook instance will be called first for all methods.
class CounterHooks : public Consensus::ConsensusFaultHooks {
 public:
  explicit CounterHooks(const std::tr1::shared_ptr<Consensus::ConsensusFaultHooks>& current_hook)
      : current_hook_(current_hook),
        pre_start_calls_(0),
        post_start_calls_(0),
        pre_config_change_calls_(0),
        post_config_change_calls_(0),
        pre_replicate_calls_(0),
        post_replicate_calls_(0),
        pre_commit_calls_(0),
        post_commit_calls_(0),
        pre_update_calls_(0),
        post_update_calls_(0),
        pre_shutdown_calls_(0),
        post_shutdown_calls_(0) {
  }

  virtual Status PreStart() OVERRIDE {
    if (current_hook_.get()) RETURN_NOT_OK(current_hook_->PreStart());
    boost::lock_guard<simple_spinlock> lock(lock_);
    pre_start_calls_++;
    return Status::OK();
  }

  virtual Status PostStart() OVERRIDE {
    if (current_hook_.get()) RETURN_NOT_OK(current_hook_->PostStart());
    boost::lock_guard<simple_spinlock> lock(lock_);
    post_start_calls_++;
    return Status::OK();
  }

  virtual Status PreConfigChange() OVERRIDE {
    if (current_hook_.get()) RETURN_NOT_OK(current_hook_->PreConfigChange());
    boost::lock_guard<simple_spinlock> lock(lock_);
    pre_config_change_calls_++;
    return Status::OK();
  }

  virtual Status PostConfigChange() OVERRIDE {
    if (current_hook_.get()) RETURN_NOT_OK(current_hook_->PostConfigChange());
    boost::lock_guard<simple_spinlock> lock(lock_);
    post_config_change_calls_++;
    return Status::OK();
  }

  virtual Status PreReplicate() OVERRIDE {
    if (current_hook_.get()) RETURN_NOT_OK(current_hook_->PreReplicate());
    boost::lock_guard<simple_spinlock> lock(lock_);
    pre_replicate_calls_++;
    return Status::OK();
  }

  virtual Status PostReplicate() OVERRIDE {
    if (current_hook_.get()) RETURN_NOT_OK(current_hook_->PostReplicate());
    boost::lock_guard<simple_spinlock> lock(lock_);
    post_replicate_calls_++;
    return Status::OK();
  }

  virtual Status PreCommit() OVERRIDE {
    if (current_hook_.get()) RETURN_NOT_OK(current_hook_->PreCommit());
    boost::lock_guard<simple_spinlock> lock(lock_);
    pre_commit_calls_++;
    return Status::OK();
  }

  virtual Status PostCommit() OVERRIDE {
    if (current_hook_.get()) RETURN_NOT_OK(current_hook_->PostCommit());
    boost::lock_guard<simple_spinlock> lock(lock_);
    post_commit_calls_++;
    return Status::OK();
  }

  virtual Status PreUpdate() OVERRIDE {
    if (current_hook_.get()) RETURN_NOT_OK(current_hook_->PreUpdate());
    boost::lock_guard<simple_spinlock> lock(lock_);
    pre_update_calls_++;
    return Status::OK();
  }

  virtual Status PostUpdate() OVERRIDE {
    if (current_hook_.get()) RETURN_NOT_OK(current_hook_->PostUpdate());
    boost::lock_guard<simple_spinlock> lock(lock_);
    post_update_calls_++;
    return Status::OK();
  }

  virtual Status PreShutdown() OVERRIDE {
    if (current_hook_.get()) RETURN_NOT_OK(current_hook_->PreShutdown());
    boost::lock_guard<simple_spinlock> lock(lock_);
    pre_shutdown_calls_++;
    return Status::OK();
  }

  virtual Status PostShutdown() OVERRIDE {
    if (current_hook_.get()) RETURN_NOT_OK(current_hook_->PostShutdown());
    boost::lock_guard<simple_spinlock> lock(lock_);
    post_shutdown_calls_++;
    return Status::OK();
  }

  int num_pre_start_calls() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return pre_start_calls_;
  }

  int num_post_start_calls() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return post_start_calls_;
  }

  int num_pre_config_change_calls() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return pre_config_change_calls_;
  }

  int num_post_config_change_calls() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return post_config_change_calls_;
  }

  int num_pre_replicate_calls() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return pre_replicate_calls_;
  }

  int num_post_replicate_calls() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return post_replicate_calls_;
  }

  int num_pre_commit_calls() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return pre_commit_calls_;
  }

  int num_post_commit_calls() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return post_commit_calls_;
  }

  int num_pre_update_calls() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return pre_update_calls_;
  }

  int num_post_update_calls() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return post_update_calls_;
  }

  int num_pre_shutdown_calls() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return pre_shutdown_calls_;
  }

  int num_post_shutdown_calls() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return post_shutdown_calls_;
  }

 private:
  std::tr1::shared_ptr<Consensus::ConsensusFaultHooks> current_hook_;
  int pre_start_calls_;
  int post_start_calls_;
  int pre_config_change_calls_;
  int post_config_change_calls_;
  int pre_replicate_calls_;
  int post_replicate_calls_;
  int pre_commit_calls_;
  int post_commit_calls_;
  int pre_update_calls_;
  int post_update_calls_;
  int pre_shutdown_calls_;
  int post_shutdown_calls_;

  // Lock that protects updates to the counters.
  mutable simple_spinlock lock_;
};

}  // namespace consensus
}  // namespace kudu

