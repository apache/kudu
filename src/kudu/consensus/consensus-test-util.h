// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <gmock/gmock.h>
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

using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using strings::Substitute;

static gscoped_ptr<ReplicateMsg> CreateDummyReplicate(int term,
                                                      int index,
                                                      int payload_size) {
    gscoped_ptr<ReplicateMsg> msg(new ReplicateMsg);
    OpId* id = msg->mutable_id();
    id->set_term(term);
    id->set_index(index);

    msg->set_op_type(NO_OP);
    msg->mutable_noop_request()->mutable_payload_for_tests()->resize(payload_size);
    return msg.Pass();
}

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
    int payload_size = 0) {

  for (int i = first; i < first + count; i++) {
    int term = i / 7;
    int index = i;
    CHECK_OK(queue->AppendOperation(CreateDummyReplicate(term, index, payload_size)));
  }
}

// Builds a quorum of 'num' elements.
// The initial roles are pre-assigned.
// The last peer (index 'num - 1') always starts out as CANDIDATE.
void BuildQuorumPBForTests(QuorumPB* quorum, int num) {
  for (int i = 0; i < num; i++) {
    QuorumPeerPB* peer_pb = quorum->add_peers();
    peer_pb->set_permanent_uuid(Substitute("peer-$0", i));
    if (i == num - 1) {
      peer_pb->set_role(QuorumPeerPB::CANDIDATE);
    } else {
      peer_pb->set_role(QuorumPeerPB::FOLLOWER);
    }
    HostPortPB* hp = peer_pb->mutable_last_known_addr();
    hp->set_host(Substitute("peer-$0.fake-domain-for-tests", i));
    hp->set_port(0);
  }
  quorum->set_local(false);
  quorum->set_seqno(0);
}

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

  explicit TestPeerProxy(ThreadPool* pool) : pool_(pool) {}

 protected:
  // Register the RPC callback in order to call later.
  // We currently only support one request of each method being in flight at a time.
  virtual void RegisterCallback(Method method, const rpc::ResponseCallback& callback) {
    lock_guard<simple_spinlock> l(&lock_);
    InsertOrDie(&callbacks_, method, callback);
  }

  // Answer the peer.
  virtual Status Respond(Method method) {
    lock_guard<simple_spinlock> l(&lock_);
    rpc::ResponseCallback callback = FindOrDie(callbacks_, method);
    CHECK_EQ(1, callbacks_.erase(method));
    return pool_->SubmitFunc(callback);
  }

  virtual Status RegisterCallbackAndRespond(Method method, const rpc::ResponseCallback& callback) {
    RegisterCallback(method, callback);
    return Respond(method);
  }

  simple_spinlock lock_;
  ThreadPool* pool_;
  std::map<Method, rpc::ResponseCallback> callbacks_; // Protected by lock_.
};

template <typename ProxyType>
class DelayablePeerProxy : public TestPeerProxy {
 public:
  // Add delayability of RPC responses to the delegated impl.
  // This class takes ownership of 'proxy'.
  explicit DelayablePeerProxy(ThreadPool* pool, ProxyType* proxy)
    : TestPeerProxy(pool),
      proxy_(CHECK_NOTNULL(proxy)),
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
    WARN_NOT_OK(TestPeerProxy::Respond(method), "Error while responding.");
  }

  virtual Status Respond(Method method) {
    latch_.Wait();   // Wait until strictly after peer would have responded.
    return TestPeerProxy::Respond(method);
  }

  virtual Status UpdateAsync(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response,
                             rpc::RpcController* controller,
                             const rpc::ResponseCallback& callback) OVERRIDE {
    RegisterCallback(kUpdate, callback);
    return proxy_->UpdateAsync(request, response, controller,
                               boost::bind(&DelayablePeerProxy::RespondUnlessDelayed,
                                           this, kUpdate));
  }

  virtual Status RequestConsensusVoteAsync(const VoteRequestPB* request,
                                           VoteResponsePB* response,
                                           rpc::RpcController* controller,
                                           const rpc::ResponseCallback& callback) OVERRIDE {
    RegisterCallback(kRequestVote, callback);
    return proxy_->RequestConsensusVoteAsync(request, response, controller,
                                             boost::bind(&DelayablePeerProxy::RespondUnlessDelayed,
                                                         this, kRequestVote));
  }

  ProxyType* proxy() const {
    return proxy_.get();
  }

 protected:
  gscoped_ptr<ProxyType> const proxy_;
  bool delay_response_; // Protected by lock_.
  CountDownLatch latch_;
};

// Allows complete mocking of a peer's responses.
// You set the response, it will respond with that.
class MockedPeerProxy : public TestPeerProxy {
 public:
  explicit MockedPeerProxy(ThreadPool* pool) : TestPeerProxy(pool) {}

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
    return RegisterCallbackAndRespond(kUpdate, callback);
  }

  virtual Status RequestConsensusVoteAsync(const VoteRequestPB* request,
                                           VoteResponsePB* response,
                                           rpc::RpcController* controller,
                                           const rpc::ResponseCallback& callback) OVERRIDE {
    *response = vote_response_;
    return RegisterCallbackAndRespond(kRequestVote, callback);
  }

 protected:
  ConsensusResponsePB update_response_;
  VoteResponsePB vote_response_;
};

// Allows to test peers by emulating a noop remote endpoint that just replies
// that the messages were received/replicated/committed.
class NoOpTestPeerProxy : public TestPeerProxy {
 public:

  explicit NoOpTestPeerProxy(ThreadPool* pool, const metadata::QuorumPeerPB& peer_pb)
    : TestPeerProxy(pool), peer_pb_(peer_pb) {
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
    return RegisterCallbackAndRespond(kUpdate, callback);
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
    return RegisterCallbackAndRespond(kRequestVote, callback);
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
  NoOpTestPeerProxyFactory() {
    CHECK_OK(ThreadPoolBuilder("test-peer-pool").set_max_threads(3).Build(&pool_));
  }

  virtual Status NewProxy(const metadata::QuorumPeerPB& peer_pb,
                          gscoped_ptr<PeerProxy>* proxy) OVERRIDE {
    proxy->reset(new NoOpTestPeerProxy(pool_.get(), peer_pb));
    return Status::OK();
  }

  gscoped_ptr<ThreadPool> pool_;
};

// Allows to test remote peers by emulating an RPC.
// Both the "remote" peer's RPC call and the caller peer's response are executed
// asynchronously in a ThreadPool.
class LocalTestPeerProxy : public TestPeerProxy {
 public:
  explicit LocalTestPeerProxy(ThreadPool* pool, Consensus* consensus)
    : TestPeerProxy(pool), consensus_(consensus),
      miss_comm_(false) {
  }

  virtual Status UpdateAsync(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response,
                             rpc::RpcController* controller,
                             const rpc::ResponseCallback& callback) OVERRIDE {
    RegisterCallback(kUpdate, callback);
    RETURN_NOT_OK(pool_->SubmitFunc(boost::bind(&LocalTestPeerProxy::SendUpdateRequest,
                                                this, request, response)));
    return Status::OK();
  }

  virtual Status RequestConsensusVoteAsync(const VoteRequestPB* request,
                                           VoteResponsePB* response,
                                           rpc::RpcController* controller,
                                           const rpc::ResponseCallback& callback) OVERRIDE {
    RegisterCallback(kRequestVote, callback);
    RETURN_NOT_OK(pool_->SubmitFunc(boost::bind(&LocalTestPeerProxy::SendVoteRequest,
                                                this, request, response)));
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
      LOG(WARNING) << "Could not Update replica with request: "
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
      WARN_NOT_OK(Respond(kUpdate), "Could not send response.");
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
      LOG(WARNING) << "Could not RequestVote from replica with request: "
          << other_peer_req.ShortDebugString()
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
      WARN_NOT_OK(Respond(kRequestVote) , "Could not send response.");
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

 private:
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
  LocalTestPeerProxyFactory() {
    CHECK_OK(ThreadPoolBuilder("test-peer-pool").set_max_threads(3).Build(&pool_));
  }
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
    LocalTestPeerProxy* new_proxy = new LocalTestPeerProxy(pool_.get(), entry.second);
    entry.first->AddPeerProxy(new_proxy);
    proxy->reset(new_proxy);
    proxies_.push_back(new_proxy);
    return Status::OK();
  }

  virtual const vector<LocalTestPeerProxy*>& GetProxies() {
    return proxies_;
  }

 private:
  gscoped_ptr<ThreadPool> pool_;
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

// Fake ReplicaTransactionFactory that allows for instantiating and unit
// testing RaftConsensusState. Does not actually support running transactions.
class MockTransactionFactory : public ReplicaTransactionFactory {
 public:
  virtual Status StartReplicaTransaction(gscoped_ptr<ConsensusRound> context) OVERRIDE {
    return StartReplicaTransactionMock(context.release());
  }
  MOCK_METHOD1(StartReplicaTransactionMock, Status(ConsensusRound* context));
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

  explicit TestRaftConsensusQueueIface(log::Log* log)
    : majority_replicated_index_(-1),
      log_(log) {
  }

  bool IsMajorityReplicated(int64_t index) {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return index <= majority_replicated_index_;
  }

  log::Log* log() const OVERRIDE { return log_; }

 protected:
  virtual void UpdateMajorityReplicated(const OpId& majority_replicated,
                                        OpId* committed_index) OVERRIDE {
    boost::lock_guard<simple_spinlock> lock(lock_);
    majority_replicated_index_ = majority_replicated.index();
    committed_index->CopyFrom(majority_replicated);
  }
  virtual void NotifyTermChange(uint64_t term) OVERRIDE {}
 private:
  mutable simple_spinlock lock_;
  int64_t majority_replicated_index_;
  log::Log* log_;
};

}  // namespace consensus
}  // namespace kudu

