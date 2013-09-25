// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_TASKS_H_
#define KUDU_CONSENSUS_TASKS_H_

#include "consensus/log.h"
#include "consensus/log.pb.h"
#include "util/task_executor.h"

namespace kudu {
namespace consensus {

//=============================================================================
//  Local Consensus Specific Tasks
//=============================================================================

// Used by LocalConsensus to simply append a ReplicateMsg to Log.
class LogTask : public Task {
 public:
  LogTask(log::Log *log,
          ReplicateMsg *msg);
  Status Run();
  bool Abort() { return false; }

 private:
  log::Log *log_;
  ReplicateMsg *msg_;
};

// Used by LocalConsensus to simply append a CommitMsg to Log.
class CommitTask : public Task {
 public:
  CommitTask(log::Log *log,
             CommitMsg *msg);
  Status Run();
  bool Abort() { return false; }

 private:
  log::Log *log_;
  CommitMsg *msg_;
};

} // namespace consensus
} // namespace kudu

#endif /* KUDU_CONSENSUS_TASKS_H_ */
