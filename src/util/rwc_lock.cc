// Copyright (c) 2013, Cloudera, inc.

#include "util/rwc_lock.h"

#include <glog/logging.h>

namespace kudu {

RWCLock::RWCLock()
  : reader_count_(0),
    write_locked_(false) {
}

RWCLock::~RWCLock() {
  CHECK_EQ(reader_count_, 0);
}

void RWCLock::ReadLock() {
  boost::lock_guard<boost::mutex> l(lock_);
  reader_count_++;
}

void RWCLock::ReadUnlock() {
  boost::lock_guard<boost::mutex> l(lock_);
  DCHECK_GT(reader_count_, 0);
  reader_count_--;
  if (reader_count_ == 0) {
    no_readers_.notify_one();
  }
}

bool RWCLock::HasReaders() const {
  boost::unique_lock<boost::mutex> l(lock_);
  return reader_count_ > 0;
}

void RWCLock::WriteLock() {
  boost::unique_lock<boost::mutex> l(lock_);
  // Wait for any other mutations to finish.
  while (write_locked_) {
    no_mutators_.wait(l);
  }
  write_locked_ = true;
}

void RWCLock::WriteUnlock() {
  boost::unique_lock<boost::mutex> l(lock_);
  DCHECK(write_locked_);
  write_locked_ = false;
  no_mutators_.notify_one();
}

void RWCLock::UpgradeToCommitLock() {
  lock_.lock();
  DCHECK(write_locked_);
  while (reader_count_ > 0) {
    no_readers_.wait(lock_);
  }
  DCHECK(write_locked_);

  // Leaves the lock held, which prevents any new readers
  // or writers.
}

void RWCLock::CommitUnlock() {
  DCHECK_EQ(0, reader_count_);
  write_locked_ = false;
  no_mutators_.notify_all();
  lock_.unlock();
}

} // namespace kudu
