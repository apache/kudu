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
    CHECK_OK(queue->AppendOperation(make_scoped_refptr_replicate(
        CreateDummyReplicate(term, index, payload_size).release())));
  }
}

// Builds a quorum of 'num' elements.
// The default behavior is to create a quorum of 'num' elements where the last element
// has a CANDIDATE role. However, if 'leader_idx' is different from -1 then that peer
// is assigned a LEADER role and the remaining ones the follower role.
void BuildQuorumPBForTests(QuorumPB* quorum, int num, int leader_idx = -1) {
  quorum->Clear();
  quorum->set_local(false);
  for (int i = 0; i < num; i++) {
    QuorumPeerPB* peer_pb = quorum->add_peers();
    peer_pb->set_permanent_uuid(Substitute("peer-$0", i));
    HostPortPB* hp = peer_pb->mutable_last_known_addr();
    hp->set_host(Substitute("peer-$0.fake-domain-for-tests", i));
    hp->set_port(0);
    if (i == leader_idx) {
      peer_pb->set_role(QuorumPeerPB::LEADER);
      continue;
    }
    if (i == num - 1 && leader_idx == -1) {
      peer_pb->set_role(QuorumPeerPB::CANDIDATE);
      continue;
    }
    peer_pb->set_role(QuorumPeerPB::FOLLOWER);
  }
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
    boost::lock_guard<simple_spinlock> lock(lock_);
    InsertOrDie(&callbacks_, method, callback);
  }

  // Answer the peer.
  virtual Status Respond(Method method) {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return RespondUnlocked(method);
  }

  virtual Status RespondUnlocked(Method method) {
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

  virtual Status Respond(Method method) OVERRIDE {
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
      // We set the last committed index to be the same index as the last received. While
      // this is unlikely to happen in a real situation, its not technically incorrect and
      // avoids having to come up with some other index that it still correct.
      response->mutable_status()->set_last_committed_idx(last_received_.index());
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

typedef std::tr1::unordered_map<std::string, scoped_refptr<RaftConsensus> > TestPeerMap;

// Thread-safe manager for list of peers being used in tests.
class TestPeerMapManager {
 public:
  explicit TestPeerMapManager(const QuorumPB& quorum) : quorum_(quorum) {}

  void AddPeer(const std::string& peer_uuid, const scoped_refptr<RaftConsensus>& peer) {
    boost::lock_guard<simple_spinlock> lock(lock_);
    InsertOrDie(&peers_, peer_uuid, peer);
  }

  Status GetPeerByIdx(int idx, scoped_refptr<RaftConsensus>* peer_out) const {
    CHECK_LT(idx, quorum_.peers_size());
    return GetPeerByUuid(quorum_.peers(idx).permanent_uuid(), peer_out);
  }

  Status GetPeerByUuid(const std::string& peer_uuid,
                       scoped_refptr<RaftConsensus>* peer_out) const {
    boost::lock_guard<simple_spinlock> lock(lock_);
    if (!FindCopy(peers_, peer_uuid, peer_out)) {
      return Status::NotFound("Other consensus instance was destroyed");
    }
    return Status::OK();
  }

  void RemovePeer(const std::string& peer_uuid) {
    boost::lock_guard<simple_spinlock> lock(lock_);
    peers_.erase(peer_uuid);
  }

  TestPeerMap GetPeerMapCopy() const {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return peers_;
  }

  void Clear() {
    // We create a copy of the peers before we clear 'peers_' so that there's
    // still a reference to each peer. If we reduce the reference count to 0 under
    // the lock we might get a deadlock as on shutdown consensus indirectly
    // destroys the test proxies which in turn reach into this class.
    TestPeerMap copy = peers_;
    {
      boost::lock_guard<simple_spinlock> lock(lock_);
      peers_.clear();
    }

  }

 private:
  const QuorumPB quorum_;
  TestPeerMap peers_;
  mutable simple_spinlock lock_;
};


// Allows to test remote peers by emulating an RPC.
// Both the "remote" peer's RPC call and the caller peer's response are executed
// asynchronously in a ThreadPool.
class LocalTestPeerProxy : public TestPeerProxy {
 public:
  LocalTestPeerProxy(const std::string& peer_uuid,
                     ThreadPool* pool,
                     TestPeerMapManager* peers)
    : TestPeerProxy(pool),
      peer_uuid_(peer_uuid),
      peers_(peers),
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

  template<class Response>
  void SetResponseError(const Status& status, Response* response) {
    tserver::TabletServerErrorPB* error = response->mutable_error();
    error->set_code(tserver::TabletServerErrorPB::UNKNOWN_ERROR);
    StatusToPB(status, error->mutable_status());
  }

  template<class Request, class Response>
  void RespondOrMissResponse(Request* request,
                             const Response& response_temp,
                             Response* final_response,
                             Method method) {

    bool miss_comm_copy;
    {
      boost::lock_guard<simple_spinlock> lock(lock_);
      miss_comm_copy = miss_comm_;
      miss_comm_ = false;
    }
    if (PREDICT_FALSE(miss_comm_copy)) {
      VLOG(2) << this << ": injecting fault on " << request->ShortDebugString();
      SetResponseError(Status::IOError("Artificial error caused by communication "
          "failure injection."), final_response);
    } else {
      final_response->CopyFrom(response_temp);
    }
    WARN_NOT_OK(Respond(method), "Could not send response.");
  }

  void SendUpdateRequest(const ConsensusRequestPB* request,
                         ConsensusResponsePB* response) {
    // Copy the request and the response for the other peer so that ownership
    // remains as close to the dist. impl. as possible.
    ConsensusRequestPB other_peer_req;
    other_peer_req.CopyFrom(*request);

    // Give the other peer a clean response object to write to.
    ConsensusResponsePB other_peer_resp;
    scoped_refptr<RaftConsensus> peer;
    Status s = peers_->GetPeerByUuid(peer_uuid_, &peer);

    if (s.ok()) {
      s = peer->Update(&other_peer_req, &other_peer_resp);
      if (s.ok() && !other_peer_resp.has_error()) {
        CHECK(other_peer_resp.has_status());
        CHECK(other_peer_resp.status().IsInitialized());
      }
    }
    if (!s.ok()) {
      LOG(WARNING) << "Could not Update replica with request: "
                   << other_peer_req.ShortDebugString()
                   << " Status: " << s.ToString();
      SetResponseError(s, &other_peer_resp);
    }

    response->CopyFrom(other_peer_resp);
    RespondOrMissResponse(request, other_peer_resp, response, kUpdate);
  }



  void SendVoteRequest(const VoteRequestPB* request,
                       VoteResponsePB* response) {

    // Copy the request and the response for the other peer so that ownership
    // remains as close to the dist. impl. as possible.
    VoteRequestPB other_peer_req;
    other_peer_req.CopyFrom(*request);
    VoteResponsePB other_peer_resp;
    other_peer_resp.CopyFrom(*response);

    scoped_refptr<RaftConsensus> peer;
    Status s = peers_->GetPeerByUuid(peer_uuid_, &peer);

    if (s.ok()) {
      s = peer->RequestVote(&other_peer_req, &other_peer_resp);
    }
    if (!s.ok()) {
      LOG(WARNING) << "Could not RequestVote from replica with request: "
                   << other_peer_req.ShortDebugString()
                   << " Status: " << s.ToString();
      SetResponseError(s, &other_peer_resp);
    }

    response->CopyFrom(other_peer_resp);
    RespondOrMissResponse(request, other_peer_resp, response, kRequestVote);
  }

  void InjectCommFaultLeaderSide() {
    VLOG(2) << this << ": injecting fault next time";
    boost::lock_guard<simple_spinlock> lock(lock_);
    miss_comm_ = true;
  }

  const std::string& GetTarget() const {
    return peer_uuid_;
  }

 private:
  const std::string peer_uuid_;
  TestPeerMapManager* const peers_;
  bool miss_comm_;
};

class LocalTestPeerProxyFactory : public PeerProxyFactory {
 public:
  explicit LocalTestPeerProxyFactory(TestPeerMapManager* peers)
    : peers_(peers) {
    CHECK_OK(ThreadPoolBuilder("test-peer-pool").set_max_threads(3).Build(&pool_));
  }

  virtual Status NewProxy(const metadata::QuorumPeerPB& peer_pb,
                          gscoped_ptr<PeerProxy>* proxy) OVERRIDE {
    LocalTestPeerProxy* new_proxy = new LocalTestPeerProxy(peer_pb.permanent_uuid(),
                                                           pool_.get(),
                                                           peers_);
    proxy->reset(new_proxy);
    proxies_.push_back(new_proxy);
    return Status::OK();
  }

  virtual const vector<LocalTestPeerProxy*>& GetProxies() {
    return proxies_;
  }

 private:
  gscoped_ptr<ThreadPool> pool_;
  TestPeerMapManager* const peers_;
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

  gscoped_ptr<ConsensusRound> round_;

 private:

  // The commit message has the exact same type of the replicate message, but
  // no content.
  void Apply() {
    gscoped_ptr<CommitMsg> msg(new CommitMsg);
    msg->set_op_type(round_->replicate_msg()->op_type());
    CHECK_OK(round_->Commit(msg.Pass(),
                            Bind(&TestDriver::CommitCallback, Unretained(this))));
  }

  void CommitCallback(const Status& s) {
    CHECK_OK(s);
    Cleanup();
  }

  ThreadPool* pool_;
};

// Fake ReplicaTransactionFactory that allows for instantiating and unit
// testing RaftConsensusState. Does not actually support running transactions.
class MockTransactionFactory : public ReplicaTransactionFactory {
 public:
  virtual Status StartReplicaTransaction(gscoped_ptr<ConsensusRound> round) OVERRIDE {
    return StartReplicaTransactionMock(round.release());
  }
  MOCK_METHOD1(StartReplicaTransactionMock, Status(ConsensusRound* round));
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

  Status StartReplicaTransaction(gscoped_ptr<ConsensusRound> round) OVERRIDE {
    TestDriver* txn = new TestDriver(pool_.get(), round.Pass());
    txn->round_->SetReplicaCommitContinuation(txn);
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

class TestRaftConsensusQueueIface : public PeerMessageQueueObserver {
 public:
  bool IsMajorityReplicated(int64_t index) {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return index <= majority_replicated_index_;
  }

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
};

}  // namespace consensus
}  // namespace kudu

