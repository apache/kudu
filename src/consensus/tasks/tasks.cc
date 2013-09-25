// Copyright (c) 2013, Cloudera, inc.

#include "consensus/tasks/tasks.h"

namespace kudu {
namespace consensus {

using std::tr1::shared_ptr;
using log::Log;
using log::LogEntry;

LogTask::LogTask(Log *log,
                 ReplicateMsg *msg)
    : log_(log),
      msg_(msg) {
}

Status LogTask::Run() {
  LogEntry entry;
  entry.set_allocated_msg(msg_);
  entry.set_type(log::REPLICATE);
  Status s = log_->Append(entry);
  entry.release_msg();
  return s;
}

CommitTask::CommitTask(Log *log,
                       CommitMsg *msg)
    : log_(log),
      msg_(msg) {
}

Status CommitTask::Run() {
  LogEntry entry;
  entry.set_allocated_commit(msg_);
  entry.set_type(log::COMMIT);
  Status s = log_->Append(entry);
  entry.release_commit();
  return s;
}

} // namespace consensus
} // namespace kudu
