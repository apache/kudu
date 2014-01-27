// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_LOCAL_CONSENSUS_TASKS_H_
#define KUDU_CONSENSUS_LOCAL_CONSENSUS_TASKS_H_

#include "consensus/log.h"
#include "consensus/log.pb.h"
#include "util/task_executor.h"

namespace kudu {
namespace consensus {

//=============================================================================
//  Local Consensus Specific Tasks
//=============================================================================

// Used by LocalConsensus to append to the Log.
class LocalConsensusLogTask : public Task {
 public:
  LocalConsensusLogTask(log::Log* log,
                        OperationPB* op);
  Status Run();
  bool Abort() { return false; }

 private:
  log::Log* log_;
  OperationPB* op_;
};

} // namespace consensus
} // namespace kudu

#endif /* KUDU_CONSENSUS_LOCAL_CONSENSUS_TASKS_H_ */
