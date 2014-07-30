// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CONSENSUS_OPID_WAITER_SET_H
#define KUDU_CONSENSUS_OPID_WAITER_SET_H

#include <map>
#include <tr1/memory>

#include "kudu/consensus/log_util.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/locks.h"

namespace kudu {

class FutureCallback;
class ThreadPool;

namespace consensus {

class OpId;

// Allows callers to register a callback which will be fired once a given
// OpId has been "finished". The meaning of "finished" is context-dependent --
// it may be "committed" or "replicated" depending on usage.
//
// This class is not thread-safe and should be externally synchronized.
class OpIdWaiterSet {
 public:
  explicit OpIdWaiterSet(ThreadPool* callback_pool);
  ~OpIdWaiterSet();

  // Registers a callback that will be triggered when the operation with 'op_id'
  // is marked as finished through MarkFinished().
  void RegisterCallback(const OpId& op_id,
                        const std::tr1::shared_ptr<FutureCallback>& callback);

  enum MarkFinishedMode {
    MARK_ALL_OPS_BEFORE,
    MARK_ONLY_THIS_OP
  };

  // Mark operations as finished and trigger the respective callbacks, if any.
  //
  // If 'mode' is 'MARK_ALL_OPS_BEFORE', then all ops before this one are also considered
  // finished. Otherwise, just this op is considered finished.
  void MarkFinished(const OpId& op_id,
                    MarkFinishedMode mode);

 private:
  typedef std::multimap<consensus::OpId,
                        std::tr1::shared_ptr<FutureCallback>,
                        log::OpIdBiggerThanFunctor > CallbackMap;

  CallbackMap callbacks_;
  ThreadPool* callback_pool_;

  DISALLOW_COPY_AND_ASSIGN(OpIdWaiterSet);
};

} // namespace consensus
} // namespace kudu
#endif /* KUDU_CONSENSUS_OPID_WAITER_SET_H */
