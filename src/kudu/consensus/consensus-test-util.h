// Copyright (c) 2013, Cloudera, inc.

#include <boost/thread/locks.hpp>
#include <string>
#include <tr1/unordered_map>
#include <tr1/memory>
#include <vector>
#include <utility>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/task_executor.h"
#include "kudu/util/threadpool.h"

#define ASSERT_OPID_EQ(left, right) do { \
  OpId left_ = (left); \
  OpId right_ = (right); \
  if (!consensus::OpIdEquals(left_, right_)) { \
    FAIL() << "Value of: " << left_.ShortDebugString() \
    << "\nExpected: " << right_.ShortDebugString(); \
  } \
  } while (0);

namespace kudu {
namespace consensus {

using strings::Substitute;

// An operation status for tests that allows to wait for operations
// to complete.
class TestOpStatusTracker : public OperationStatusTracker {
 public:
  TestOpStatusTracker(gscoped_ptr<ReplicateMsg> op, int n_majority, int total_peers)
    : OperationStatusTracker(op.Pass()),
      majority_latch_(n_majority),
      all_replicated_latch_(total_peers),
      replicated_count_(0) {
  }

  void AckPeer(const string& uuid) OVERRIDE {
    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG(2) << "Peer: " << uuid << " Ack'd op: " << replicate_msg_->ShortDebugString();
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
                      replicate_msg_->ShortDebugString(), IsDone(), IsAllDone(), replicated_count_);
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
    gscoped_ptr<ReplicateMsg> msg(new ReplicateMsg);
    OpId* id = msg->mutable_id();
    id->set_term(i / 7);
    id->set_index(i);
    msg->set_op_type(NO_OP);
    msg->mutable_noop_request()->set_payload_for_tests(dummy_payload);
    scoped_refptr<OperationStatusTracker> status(
        new TestOpStatusTracker(msg.Pass(), n_majority, total_peers));
    CHECK_OK(queue->AppendOperation(status));
    if (statuses_collector) {
      statuses_collector->push_back(status);
    }
  }
}

// Task to emulate the exactly-once calling behavior of RPC.
// Sets an error in the response if the task is aborted and guarantees that
// the callback will be invoked.
// TODO: Should set an error in the controller but that is not currently
// possible to mock. This should all probably go away and just use real RPCs.
template<typename ResponseType>
class MockRpcExactlyOnceTask : public Task {
 public:
  MockRpcExactlyOnceTask(ResponseType* response,
                         const rpc::ResponseCallback& callback)
    : response_(DCHECK_NOTNULL(response)),
      callback_(callback) {
  }

  virtual Status Run() OVERRIDE {
    callback_();
    return Status::OK();
  }

  virtual bool Abort() OVERRIDE {
    // TODO: Provide some way to mock a true RPC error in the RpcController?
    tserver::TabletServerErrorPB* error = response_->mutable_error();
    error->set_code(tserver::TabletServerErrorPB::UNKNOWN_ERROR);
    StatusToPB(Status::IOError("Mocked RPC failure due to TaskExecutor shutdown."),
               error->mutable_status());
    callback_();
    return true;
  }

 private:
  ResponseType* const response_;
  const rpc::ResponseCallback callback_;
};

// Allows to test peers by emulating a noop remote endpoint that just replies
// that the messages were received/replicated/committed.
class NoOpTestPeerProxy : public PeerProxy {
 public:
  explicit NoOpTestPeerProxy(const metadata::QuorumPeerPB& peer_pb)
    : delay_response_(false),
      callback_(NULL),
      peer_pb_(peer_pb) {
    CHECK_OK(TaskExecutorBuilder("noop-peer-pool").set_max_threads(1).Build(&executor_));
    last_received_.CopyFrom(MinimumOpId());
  }

  virtual Status UpdateAsync(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response,
                             rpc::RpcController* controller,
                             const rpc::ResponseCallback& callback) OVERRIDE {

    response->Clear();

    boost::lock_guard<simple_spinlock> lock(lock_);
    if (OpIdLessThan(last_received_, request->preceding_id())) {
      ConsensusErrorPB* error = response->mutable_status()->mutable_error();
      error->set_code(ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH);
      StatusToPB(Status::IllegalState(""), error->mutable_status());
    } else if (request->ops_size() > 0) {
      last_received_.CopyFrom(request->ops(request->ops_size() - 1).id());
    }

    response->set_responder_uuid(peer_pb_.permanent_uuid());
    response->set_responder_term(request->caller_term());
    response->mutable_status()->mutable_last_received()->CopyFrom(last_received_);

    update_task_.reset(new MockRpcExactlyOnceTask<ConsensusResponsePB>(response, callback));

    if (!delay_response_) {
      RETURN_NOT_OK(RespondUnlocked());
    }

    return Status::OK();
  }

  const OpId& last_received() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return last_received_;
  }

  // Delays the answer to the next response to this remote
  // peer. The response callback will only be called on Respond().
  void DelayResponse() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    delay_response_ = true;
  }

  // Answers the peer.
  Status Respond() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return RespondUnlocked();
  }

 private:
  Status RespondUnlocked() {
    delay_response_ = false;
    return executor_->Submit(update_task_, NULL);
  }

  gscoped_ptr<TaskExecutor> executor_;
  OpId last_received_;
  bool delay_response_;
  rpc::ResponseCallback callback_;
  std::tr1::shared_ptr<Task> update_task_;
  const metadata::QuorumPeerPB peer_pb_;
  mutable simple_spinlock lock_;
};

class NoOpTestPeerProxyFactory : public PeerProxyFactory {
 public:

  virtual Status NewProxy(const metadata::QuorumPeerPB& peer_pb,
                          gscoped_ptr<PeerProxy>* proxy) OVERRIDE {
    proxy->reset(new NoOpTestPeerProxy(peer_pb));
    return Status::OK();
  }
};

// Allows to test remote peers by emulating an RPC.
// Both the "remote" peer's RPC call and the caller peer's response are executed
// asynchronously in a TaskExecutor.
class LocalTestPeerProxy : public PeerProxy {
 public:
  explicit LocalTestPeerProxy(Consensus* consensus)
    : consensus_(consensus),
      miss_comm_(false) {
    CHECK_OK(TaskExecutorBuilder("noop-peer-pool").set_max_threads(1).Build(&executor_));
  }

  virtual Status UpdateAsync(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response,
                             rpc::RpcController* controller,
                             const rpc::ResponseCallback& callback) OVERRIDE {
    boost::lock_guard<simple_spinlock> lock(lock_);
    update_task_.reset(new MockRpcExactlyOnceTask<ConsensusResponsePB>(response, callback));
    std::tr1::shared_ptr<Task> processor(
        new MockRpcExactlyOnceTask<ConsensusResponsePB>(response,
            boost::bind(&LocalTestPeerProxy::SendToOtherPeer,
                        this, request, response)));
    RETURN_NOT_OK(executor_->Submit(processor, NULL));
    return Status::OK();
  }

  void SendToOtherPeer(const ConsensusRequestPB* request,
                       ConsensusResponsePB* response) {
    // Copy the request for the other peer so that ownership
    // remains as close to the dist. impl. as possible.
    ConsensusRequestPB other_peer_req;
    other_peer_req.CopyFrom(*request);

    // Give the other peer a clean response object to write to.
    ConsensusResponsePB other_peer_resp;
    Status s;
    {
      boost::lock_guard<simple_spinlock> lock(lock_);
      if (PREDICT_TRUE(consensus_)) {
        s = consensus_->Update(&other_peer_req, &other_peer_resp);
        if (s.ok() && !other_peer_resp.has_error()) {
          CHECK(other_peer_resp.has_status());
          CHECK(other_peer_resp.status().IsInitialized());
        }
      } else {
        s = Status::NotFound("Other consensus instance was destroyed");
      }
    }
    if (!s.ok()) {
      LOG(WARNING) << "Could not update replica with request:"
                   << other_peer_req.ShortDebugString()
                   << " Status: " << s.ToString();
      tserver::TabletServerErrorPB* error = other_peer_resp.mutable_error();
            error->set_code(tserver::TabletServerErrorPB::UNKNOWN_ERROR);
            StatusToPB(s, error->mutable_status());
    }
    {
      boost::lock_guard<simple_spinlock> lock(lock_);
      if (PREDICT_FALSE(miss_comm_)) {
        VLOG(2) << this << ": injecting fault on " << request->ShortDebugString();
        tserver::TabletServerErrorPB* error = response->mutable_error();
        error->set_code(tserver::TabletServerErrorPB::UNKNOWN_ERROR);
        StatusToPB(Status::IOError("Artificial error caused by communication failure injection."),
                   error->mutable_status());
        miss_comm_ = false;
      } else {
        response->CopyFrom(other_peer_resp);
        VLOG(2) << this << ": not injecting fault for req: " << request->ShortDebugString();
      }
      CHECK_OK(executor_->Submit(update_task_, NULL));
    }
  }

  void ConsensusShutdown() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    consensus_ = NULL;
  }

  void InjectCommFaultLeaderSide() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    VLOG(2) << this << ": injecting fault next time";
    miss_comm_ = true;
  }

  Consensus* GetTarget() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return consensus_;
  }

  ~LocalTestPeerProxy() {
    executor_->Wait();
  }

 private:
  gscoped_ptr<TaskExecutor> executor_;
  std::tr1::shared_ptr<Task> update_task_;
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

// A simple implementation of ReplicaCommitContinuation for tests.
// This is usually implemented by ReplicaTransactionDriver but here
// we limit the implementation to the minimally required to have consensus
// work.
class TestReplicaDriver : public ReplicaCommitContinuation {
 public:
  TestReplicaDriver(ThreadPool* pool, gscoped_ptr<ConsensusRound> round)
      : round_(round.Pass()),
        pool_(pool) {
  }

  virtual Status ConsensusCommitted() OVERRIDE {
    RETURN_NOT_OK(pool_->SubmitFunc(boost::bind(&TestReplicaDriver::ReplicaCommit, this)));
    return Status::OK();
  }

  void ReplicaCommit() {
    // Normally the replica would have a different commit msg
    // but for tests we just copy the leader's message.
    gscoped_ptr<CommitMsg> msg(new CommitMsg);
    msg->set_op_type(NO_OP);
    CHECK_OK(round_->Commit(msg.Pass()));
  }

  // Called in all modes to delete the transaction and, transitively, the consensus
  // round.
  void Cleanup() {
    delete this;
  }

  virtual void Abort() { Cleanup(); }

  void Fatal(const Status& status) {
    LOG(FATAL) << "TestReplicaDriver aborted with status: " << status.ToString();
  }

  gscoped_ptr<ConsensusRound> round_;

 private:
  ThreadPool* pool_;
};

// A simple implementation of the leader driver for transactions.
// This is usually implemented by LeaderTransactionDriver but here we
// keep the implementation to the minimally required to have consensus
// work.
class TestLeaderDriver {
 public:
  explicit TestLeaderDriver(ThreadPool* pool)
      : pool_(pool) {
  }

  void SetRound(gscoped_ptr<ConsensusRound> round) {
    round_.reset(round.release());
  }

  // Does nothing but enqueue the commit, emulating an Apply
  void TransactionReplicated() {
    CHECK_OK(
        pool_->SubmitFunc(boost::bind(&TestLeaderDriver::LeaderCommit, this)));
  }

  // The commit message has the exact same type of the replicate message, but
  // no content.
  void LeaderCommit() {
    gscoped_ptr<CommitMsg> msg(new CommitMsg);
    msg->set_op_type(round_->replicate_msg()->op_type());
    CHECK_OK(round_->Commit(msg.Pass()));
  }

  // Called in all modes to delete the transaction and, transitively, the consensus
  // round.
  void Cleanup() {
    delete this;
  }

  void Abort() {}

  void Fatal(const Status& status) {
    LOG(FATAL) << "TestReplicaDriver aborted with status: " << status.ToString();
  }

  gscoped_ptr<ConsensusRound> round_;

 private:
  ThreadPool* pool_;
};

// Fake, no-op ReplicaTransactionFactory that allows for instantiating and unit
// testing RaftConsensusState. Does not actually support running transactions.
class MockTransactionFactory : public ReplicaTransactionFactory {
 public:
  Status StartReplicaTransaction(gscoped_ptr<ConsensusRound> context) OVERRIDE {
    return Status::OK();
  }
  Status SubmitConsensusChangeConfig(gscoped_ptr<metadata::QuorumPB> quorum,
                                     const StatusCallback& callback) OVERRIDE {

    return Status::OK();
  }
};

// A transaction factory for tests, usually this is implemented by TabletPeer.
class TestTransactionFactory : public ReplicaTransactionFactory {
 public:
  TestTransactionFactory() : consensus_(NULL) {
    CHECK_OK(ThreadPoolBuilder("test-txn-factory").set_max_threads(1).Build(&pool_));
  }

  void SetConsensus(Consensus* consensus) {
    consensus_ = consensus;
  }

  Status StartReplicaTransaction(gscoped_ptr<ConsensusRound> context) OVERRIDE {
    TestReplicaDriver* txn = new TestReplicaDriver(pool_.get(), context.Pass());
    txn->round_->SetReplicaCommitContinuation(txn);
    std::tr1::shared_ptr<FutureCallback> commit_clbk(
        new BoundFunctionCallback(boost::bind(&TestReplicaDriver::Cleanup, txn),
                                  boost::bind(&TestReplicaDriver::Fatal, txn, _1)));
    txn->round_->SetCommitCallback(commit_clbk);
    return Status::OK();
  }

  Status SubmitConsensusChangeConfig(gscoped_ptr<metadata::QuorumPB> quorum,
                                     const StatusCallback& callback) OVERRIDE {
    gscoped_ptr<ReplicateMsg> replicate_msg(new ReplicateMsg);
    replicate_msg->set_op_type(CHANGE_CONFIG_OP);
    consensus::ChangeConfigRequestPB* cc_request = replicate_msg->mutable_change_config_request();
    cc_request->mutable_new_config()->CopyFrom(*quorum);
    cc_request->set_tablet_id("");

    TestLeaderDriver* test_transaction = new TestLeaderDriver(pool_.get());

    std::tr1::shared_ptr<FutureCallback> replicate_callback(
        new BoundFunctionCallback(boost::bind(&TestLeaderDriver::TransactionReplicated,
                                              test_transaction),
                                  boost::bind(&TestLeaderDriver::Fatal, test_transaction, _1)));

    std::tr1::shared_ptr<FutureCallback> commit_clbk(
        new BoundFunctionCallback(boost::bind(&TestLeaderDriver::Cleanup, test_transaction),
                                  boost::bind(&TestLeaderDriver::Fatal, test_transaction, _1)));

    gscoped_ptr<ConsensusRound> round(new ConsensusRound(consensus_,
                                                         replicate_msg.Pass(),
                                                         replicate_callback,
                                                         commit_clbk));

    test_transaction->SetRound(round.Pass());

    RETURN_NOT_OK(pool_->SubmitFunc(boost::bind(&TestTransactionFactory::ReplicateAsync,
                                                this,
                                                test_transaction->round_.get())));
    return Status::OK();
  }

  void ReplicateAsync(ConsensusRound* round) {
    CHECK_OK(consensus_->Replicate(round));
  }

  void WaitDone() {
    pool_->Wait();
  }

  void ShutDown() {
    WaitDone();
    pool_->Shutdown();
  }

  ~TestTransactionFactory() {
    ShutDown();
  }

 private:
  gscoped_ptr<ThreadPool> pool_;
  Consensus* consensus_;
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

class MockRaftConsensusQueueIface : public RaftConsensusQueueIface {
 protected:
  virtual void UpdateCommittedIndex(const OpId&) OVERRIDE {}
};

}  // namespace consensus
}  // namespace kudu

