// Copyright (c) 2013, Cloudera, inc.

#include <string>
#include <vector>

#include "consensus/consensus_queue.h"
#include "util/countdown_latch.h"

namespace kudu {
namespace consensus {

// An operation status for tests that allows to wait for operations
// to complete.
class TestOperationStatus : public OperationStatus {
 public:
  explicit TestOperationStatus(int n_majority)
      : n_majority_(n_majority),
        latch_(n_majority) {
  }
  void AckPeer(const string& uuid) {
    latch_.CountDown();
  }
  bool IsDone() {
    return latch_.count() >= n_majority_;
  }
  void Wait() {
    latch_.Wait();
  }

 private:
  int n_majority_;
  CountDownLatch latch_;
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
    int count,
    int n_majority = 1,
    vector<scoped_refptr<OperationStatus> >* statuses_collector = NULL) {

  for (int i = 0; i < count; i++) {
    gscoped_ptr<OperationPB> op(new OperationPB);
    OpId* id = op->mutable_id();
    id->set_term(i / 7);
    id->set_index(i % 7);
    scoped_refptr<OperationStatus> status(new TestOperationStatus(n_majority));
    queue->AppendOperation(op.Pass(), status);
    if (statuses_collector) {
      statuses_collector->push_back(status);
    }
  }
}

}  // namespace consensus
}  // namespace kudu

