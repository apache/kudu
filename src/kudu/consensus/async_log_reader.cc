// Copyright (c) 2013, Cloudera, inc.

#include <boost/bind.hpp>
#include <boost/thread/locks.hpp>

#include "kudu/consensus/async_log_reader.h"

#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/log_reader.h"

namespace kudu {
namespace log {

using consensus::OpId;
using consensus::ReplicateMsg;
using std::vector;

Status AsyncLogReader::EnqueueAsyncRead(const OpId& starting_after,
                                        const OpId& up_to,
                                        const ReadDoneCallback& callback) {
  DCHECK(consensus::OpIdLessThan(starting_after, up_to));

  boost::lock_guard<simple_spinlock> lock(lock_);

  if (is_reading_) {
    return Status::AlreadyPresent("The QueueLoader is already loading the queue.");
  }

  RETURN_NOT_OK(pool_->SubmitFunc(boost::bind(&AsyncLogReader::ReadEntriesAsync,
                                              this,
                                              starting_after,
                                              up_to,
                                              callback)));
  is_reading_ = true;
  run_latch_.Reset(1);

  return Status::OK();
}

void AsyncLogReader::ReadEntriesAsync(const OpId& starting_after,
                                      const OpId& up_to,
                                      const ReadDoneCallback& callback) {

  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    DCHECK(is_reading_);
  }

  vector<ReplicateMsg*> replicates;
  Status s = log_reader_->ReadAllReplicateEntries(starting_after,
                                                  up_to,
                                                  &replicates);
  callback.Run(s, replicates, starting_after);

  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    is_reading_ = false;
    run_latch_.CountDown();
  }
}

bool AsyncLogReader::IsReading() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return is_reading_;
}

void AsyncLogReader::Shutdown() {
  run_latch_.Wait();
}

}  // namespace log
}  // namespace kudu
