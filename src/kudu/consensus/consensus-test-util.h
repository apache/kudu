// Copyright (c) 2013, Cloudera, inc.

#include <boost/thread/locks.hpp>
#include <map>
#include <string>
#include <tr1/unordered_map>
#include <tr1/memory>
#include <vector>
#include <utility>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/opid_waiter_set.h"
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
    string dummy_payload = "") {

  for (int i = first; i < first + count; i++) {
    gscoped_ptr<ReplicateMsg> msg(new ReplicateMsg);
    OpId* id = msg->mutable_id();
    id->set_term(i / 7);
    id->set_index(i);
    msg->set_op_type(NO_OP);
    msg->mutable_noop_request()->set_payload_for_tests(dummy_payload);
    CHECK_OK(queue->AppendOperation(msg.Pass()));
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

// Abstract base class to build PeerProxy implementations on top of for testing.
// Provides a single-threaded pool to run callbacks in and callback
// registration/running, along with an enum to identify the supported methods.
class TestPeerProxy : public PeerProxy {
 public:
  // Which PeerProxy method to invoke.
  enum Method {
    kUpdate,
    kRequestVote,
  };

  TestPeerProxy() {
    CHECK_OK(TaskExecutorBuilder("test-peer-pool").set_max_threads(1).Build(&executor_));
  }

 protected:
  // Register the RPC callback in order to call later.
  // We currently only support one request of each method being in flight at a time.
  virtual void RegisterCallback(Method method, const std::tr1::shared_ptr<Task>& task) {
    lock_guard<simple_spinlock> l(&lock_);
    InsertOrDie(&tasks_, method, task);
  }

  // Answer the peer.
  virtual Status Respond(Method method) {
    lock_guard<simple_spinlock> l(&lock_);
    std::tr1::shared_ptr<Task> task = FindOrDie(tasks_, method);
    CHECK_EQ(1, tasks_.erase(method));
    return executor_->Submit(task, NULL);
  }

  virtual Status RegisterCallbackAndRespond(Method method, const std::tr1::shared_ptr<Task>& task) {
    RegisterCallback(method, task);
    return Respond(method);
  }

  simple_spinlock lock_;
  gscoped_ptr<TaskExecutor> executor_;
  std::map<Method, std::tr1::shared_ptr<Task> > tasks_; // Protected by lock_.
};

template <typename ProxyType>
class DelayablePeerProxy : public TestPeerProxy {
 public:
  // Add delayability of RPC responses to the delegated impl.
  // This class takes ownership of 'proxy'.
  explicit DelayablePeerProxy(ProxyType* proxy)
    : proxy_(CHECK_NOTNULL(proxy)),
      delay_response_(false),
      latch_(1) {
  }

  // Delay the answer to the next response to this remote
  // peer. The response callback will only be called on Respond().
  virtual void DelayResponse() {
    lock_guard<simple_spinlock> l(&lock_);
    delay_response_ = true;
    latch_.Reset(1); // Reset for the next time.
  }

  virtual void RespondUnlessDelayed(Method method) {
    {
      lock_guard<simple_spinlock> l(&lock_);
      if (delay_response_) {
        latch_.CountDown();
        delay_response_ = false;
        return;
      }
    }
    CHECK_OK(TestPeerProxy::Respond(method));
  }

  virtual Status Respond(Method method) {
    latch_.Wait();   // Wait until strictly after peer would have responded.
    Status s = TestPeerProxy::Respond(method);
    return s;
  }

  virtual Status UpdateAsync(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response,
                             rpc::RpcController* controller,
                             const rpc::ResponseCallback& callback) OVERRIDE {
    std::tr1::shared_ptr<Task> task(
        new MockRpcExactlyOnceTask<ConsensusResponsePB>(response, callback));
    RegisterCallback(kUpdate, task);
    return proxy_->UpdateAsync(request, response, controller,
                               boost::bind(&DelayablePeerProxy::RespondUnlessDelayed,
                                           this, kUpdate));
  }

  virtual Status RequestConsensusVoteAsync(const VoteRequestPB* request,
                                           VoteResponsePB* response,
                                           rpc::RpcController* controller,
                                           const rpc::ResponseCallback& callback) OVERRIDE {
    std::tr1::shared_ptr<Task> task(
        new MockRpcExactlyOnceTask<VoteResponsePB>(response, callback));
    RegisterCallback(kRequestVote, task);
    return proxy_->RequestConsensusVoteAsync(request, response, controller,
                                             boost::bind(&DelayablePeerProxy::RespondUnlessDelayed,
                                                         this, kRequestVote));
  }

  ProxyType* proxy() const {
    return proxy_.get();
  }

 protected:
  simple_spinlock lock_;
  gscoped_ptr<ProxyType> const proxy_;
  bool delay_response_; // Protected by lock_.
  CountDownLatch latch_;
};

// Allows complete mocking of a peer's responses.
// You set the response, it will respond with that.
class MockedPeerProxy : public TestPeerProxy {
 public:
  MockedPeerProxy() {
  }

  virtual void set_update_response(const ConsensusResponsePB& update_response) {
    update_response_ = update_response;
  }

  virtual void set_vote_response(const VoteResponsePB& vote_response) {
    vote_response_ = vote_response;
  }

  virtual Status UpdateAsync(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response,
                             rpc::RpcController* controller,
                             const rpc::ResponseCallback& callback) OVERRIDE {
    *response = update_response_;
    std::tr1::shared_ptr<Task> task(
        new MockRpcExactlyOnceTask<ConsensusResponsePB>(response, callback));
    return RegisterCallbackAndRespond(kUpdate, task);
  }

  virtual Status RequestConsensusVoteAsync(const VoteRequestPB* request,
                                           VoteResponsePB* response,
                                           rpc::RpcController* controller,
                                           const rpc::ResponseCallback& callback) OVERRIDE {
    *response = vote_response_;
    std::tr1::shared_ptr<Task> task(
        new MockRpcExactlyOnceTask<VoteResponsePB>(response, callback));
    return RegisterCallbackAndRespond(kRequestVote, task);
  }

 protected:
  ConsensusResponsePB update_response_;
  VoteResponsePB vote_response_;
};

// Allows to test peers by emulating a noop remote endpoint that just replies
// that the messages were received/replicated/committed.
class NoOpTestPeerProxy : public TestPeerProxy {
 public:

  explicit NoOpTestPeerProxy(const metadata::QuorumPeerPB& peer_pb)
    : peer_pb_(peer_pb) {
    last_received_.CopyFrom(MinimumOpId());
  }

  virtual Status UpdateAsync(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response,
                             rpc::RpcController* controller,
                             const rpc::ResponseCallback& callback) OVERRIDE {

    response->Clear();
    {
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
    }
    std::tr1::shared_ptr<Task> task(
        new MockRpcExactlyOnceTask<ConsensusResponsePB>(response, callback));
    return RegisterCallbackAndRespond(kUpdate, task);
  }

  virtual Status RequestConsensusVoteAsync(const VoteRequestPB* request,
                                           VoteResponsePB* response,
                                           rpc::RpcController* controller,
                                           const rpc::ResponseCallback& callback) OVERRIDE {
    {
      boost::lock_guard<simple_spinlock> lock(lock_);
      response->set_responder_uuid(peer_pb_.permanent_uuid());
      response->set_responder_term(request->candidate_term());
      response->set_vote_granted(true);
    }
    std::tr1::shared_ptr<Task> task(
        new MockRpcExactlyOnceTask<VoteResponsePB>(response, callback));
    return RegisterCallbackAndRespond(kRequestVote, task);
  }

  const OpId& last_received() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return last_received_;
  }

 private:
  const metadata::QuorumPeerPB peer_pb_;
  ConsensusStatusPB last_status_; // Protected by lock_.
  OpId last_received_;            // Protected by lock_.
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
class LocalTestPeerProxy : public TestPeerProxy {
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
    // Register callback.
    std::tr1::shared_ptr<Task> task(
        new MockRpcExactlyOnceTask<ConsensusResponsePB>(response, callback));
    RegisterCallback(kUpdate, task);

    // Run mock processing on another thread.
    std::tr1::shared_ptr<Task> processor(
        new MockRpcExactlyOnceTask<ConsensusResponsePB>(response,
            boost::bind(&LocalTestPeerProxy::SendUpdateRequest,
                        this, request, response)));
    RETURN_NOT_OK(executor_->Submit(processor, NULL));
    return Status::OK();
  }

  virtual Status RequestConsensusVoteAsync(const VoteRequestPB* request,
                                           VoteResponsePB* response,
                                           rpc::RpcController* controller,
                                           const rpc::ResponseCallback& callback) OVERRIDE {
    std::tr1::shared_ptr<Task> task(
        new MockRpcExactlyOnceTask<VoteResponsePB>(response, callback));
    RegisterCallback(kRequestVote, task);

    std::tr1::shared_ptr<Task> processor(
        new MockRpcExactlyOnceTask<VoteResponsePB>(response,
            boost::bind(&LocalTestPeerProxy::SendVoteRequest,
                        this, request, response)));
    RETURN_NOT_OK(executor_->Submit(processor, NULL));
    return Status::OK();
  }

  void SendUpdateRequest(const ConsensusRequestPB* request,
                         ConsensusResponsePB* response) {
    // Copy the request and the response for the other peer so that ownership
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
      CHECK_OK(Respond(kUpdate));
    }
  }

  void SendVoteRequest(const VoteRequestPB* request,
                       VoteResponsePB* response) {

    // Copy the request and the response for the other peer so that ownership
    // remains as close to the dist. impl. as possible.
    VoteRequestPB other_peer_req;
    other_peer_req.CopyFrom(*request);
    VoteResponsePB other_peer_resp;
    other_peer_resp.CopyFrom(*response);

    // FIXME: This is one big massive copy & paste.
    Status s;
    {
      boost::lock_guard<simple_spinlock> lock(lock_);
      if (PREDICT_TRUE(consensus_)) {
        s = consensus_->RequestVote(&other_peer_req, &other_peer_resp);
      } else {
        s = Status::NotFound("Other consensus instance was destroyed");
      }
    }
    if (!s.ok()) {
      LOG(WARNING) << "Could not update replica "
          << ". With request: " << other_peer_req.ShortDebugString()
          << " Status: " << s.ToString();
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
      CHECK_OK(Respond(kRequestVote));
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

// A simple implementation of the transaction driver.
// This is usually implemented by TransactionDriver but here we
// keep the implementation to the minimally required to have consensus
// work.
class TestDriver : public ConsensusCommitContinuation {
 public:
  explicit TestDriver(ThreadPool* pool)
      : pool_(pool) {
  }

  TestDriver(ThreadPool* pool, gscoped_ptr<ConsensusRound> round)
      : round_(round.Pass()),
        pool_(pool) {
  }

  void SetRound(gscoped_ptr<ConsensusRound> round) {
    round_.reset(round.release());
  }

  // Does nothing but enqueue the Apply
  virtual void ReplicationFinished(const Status& status) OVERRIDE {
    if (status.IsAborted()) {
      Cleanup();
      return;
    }
    CHECK_OK(status);
    CHECK_OK(pool_->SubmitFunc(boost::bind(&TestDriver::Apply, this)));
  }

  // Called in all modes to delete the transaction and, transitively, the consensus
  // round.
  void Cleanup() {
    delete this;
  }

  void Fatal(const Status& status) {
    LOG(FATAL) << "TestDriver aborted with status: " << status.ToString();
  }

  gscoped_ptr<ConsensusRound> round_;

 private:

  // The commit message has the exact same type of the replicate message, but
  // no content.
  void Apply() {
    gscoped_ptr<CommitMsg> msg(new CommitMsg);
    msg->set_op_type(round_->replicate_msg()->op_type());
    CHECK_OK(round_->Commit(msg.Pass()));
  }

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
    TestDriver* txn = new TestDriver(pool_.get(), context.Pass());
    txn->round_->SetReplicaCommitContinuation(txn);
    std::tr1::shared_ptr<FutureCallback> commit_clbk(
        new BoundFunctionCallback(boost::bind(&TestDriver::Cleanup, txn),
                                  boost::bind(&TestDriver::Fatal, txn, _1)));
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

    TestDriver* test_transaction = new TestDriver(pool_.get());

    std::tr1::shared_ptr<FutureCallback> commit_clbk(
        new BoundFunctionCallback(boost::bind(&TestDriver::Cleanup, test_transaction),
                                  boost::bind(&TestDriver::Fatal, test_transaction, _1)));

    gscoped_ptr<ConsensusRound> round(new ConsensusRound(consensus_,
                                                         replicate_msg.Pass(),
                                                         test_transaction,
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

class TestRaftConsensusQueueIface : public RaftConsensusQueueIface {
 public:
  TestRaftConsensusQueueIface() {
    ThreadPoolBuilder("ci-waiters").set_max_threads(1).Build(&pool_);
    committed_waiter_set_.reset(new OpIdWaiterSet(pool_.get()));
  }

  void RegisterCallback(const OpId& op_id,
                        const std::tr1::shared_ptr<FutureCallback>& callback) {
    boost::lock_guard<simple_spinlock> lock(lock_);
    committed_waiter_set_->RegisterCallback(op_id, callback);
  }
 protected:
  virtual void UpdateCommittedIndex(const OpId& id) OVERRIDE {
    boost::lock_guard<simple_spinlock> lock(lock_);
    committed_waiter_set_->MarkFinished(id, OpIdWaiterSet::MARK_ALL_OPS_BEFORE);
  }
  virtual void NotifyTermChange(uint64_t term) OVERRIDE {}
 private:
  gscoped_ptr<ThreadPool> pool_;
  gscoped_ptr<OpIdWaiterSet> committed_waiter_set_;
  mutable simple_spinlock lock_;
};

}  // namespace consensus
}  // namespace kudu

