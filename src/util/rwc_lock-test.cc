// Copyright (c) 2013, Cloudera, inc.

#include "util/rwc_lock.h"
#include "util/test_util.h"
#include "util/locks.h"

#include <boost/thread/thread.hpp>
#include <boost/foreach.hpp>

namespace kudu {

class RWCLockTest : public KuduTest {};

// Holds counters of how many threads hold the lock in each of the
// provided modes.
struct LockHoldersCount {
  LockHoldersCount()
    : num_readers(0),
      num_writers(0),
      num_committers(0) {
  }

  // Check the invariants of the lock counts.
  void CheckInvariants() {
    // At no time should we have more than one writer or committer.
    CHECK_LE(num_writers, 1);
    CHECK_LE(num_committers, 1);

    // If we have any readers, then we should not have any committers.
    if (num_readers > 0) {
      CHECK_EQ(num_committers, 0);
    }
  }

  void AdjustReaders(int delta) {
    boost::lock_guard<simple_spinlock> l(lock);
    num_readers += delta;
    CheckInvariants();
  }

  void AdjustWriters(int delta) {
    boost::lock_guard<simple_spinlock> l(lock);
    num_writers += delta;
    CheckInvariants();
  }

  void AdjustCommitters(int delta) {
    boost::lock_guard<simple_spinlock> l(lock);
    num_committers += delta;
    CheckInvariants();
  }

  int num_readers;
  int num_writers;
  int num_committers;
  simple_spinlock lock;
};

struct SharedState {
  LockHoldersCount counts;
  RWCLock rwc_lock;
  volatile bool stop;
};

void ReaderThread(SharedState* state) {
  while (!state->stop) {
    state->rwc_lock.ReadLock();
    state->counts.AdjustReaders(1);
    state->counts.AdjustReaders(-1);
    state->rwc_lock.ReadUnlock();
  }
}

void WriterThread(SharedState* state) {
  string local_str;
  while (!state->stop) {
    state->rwc_lock.WriteLock();
    state->counts.AdjustWriters(1);

    state->rwc_lock.UpgradeToCommitLock();
    state->counts.AdjustWriters(-1);
    state->counts.AdjustCommitters(1);

    state->counts.AdjustCommitters(-1);
    state->rwc_lock.CommitUnlock();
  }
}


TEST_F(RWCLockTest, TestCorrectBehavior) {
  SharedState state;
  state.stop = false;

  vector<boost::thread*> threads;

  const int kNumWriters = 5;
  const int kNumReaders = 5;

  for (int i = 0; i < kNumWriters; i++) {
    threads.push_back(new boost::thread(WriterThread, &state));
  }
  for (int i = 0; i < kNumReaders; i++) {
    threads.push_back(new boost::thread(ReaderThread, &state));
  }

  if (AllowSlowTests()) {
    sleep(1);
  } else {
    usleep(100 * 1000);
  }

  state.stop = true;

  BOOST_FOREACH(boost::thread* t, threads) {
    t->join();
    delete t;
  }

}

} // namespace kudu
