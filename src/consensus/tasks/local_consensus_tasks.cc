// Copyright (c) 2013, Cloudera, inc.

#include "consensus/tasks/local_consensus_tasks.h"

namespace kudu {
namespace consensus {

using std::tr1::shared_ptr;
using log::Log;
using log::LogEntry;

LocalConsensusLogTask::LocalConsensusLogTask(Log *log,
                                             OperationPB* op)
    : log_(log),
      op_(op) {
}

Status LocalConsensusLogTask::Run() {
  LogEntry entry;
  entry.set_type(log::OPERATION);
  entry.set_allocated_operation(op_);
  Status s = log_->Append(entry);
  entry.release_operation();
  return s;
}

}  // namespace consensus
}  // namespace kudu
