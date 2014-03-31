// Copyright (c) 2014, Cloudera, inc.

#include "consensus/opid_waiter_set.h"

#include <glog/logging.h>
#include <tr1/memory>

#include "consensus/log_util.h"
#include "gutil/map-util.h"
#include "util/status.h"
#include "util/task_executor.h"
#include "util/threadpool.h"

using std::tr1::shared_ptr;

namespace kudu {
namespace consensus {

OpIdWaiterSet::OpIdWaiterSet(ThreadPool* callback_pool)
  : callback_pool_(callback_pool) {
}

OpIdWaiterSet::~OpIdWaiterSet() {
}

void OpIdWaiterSet::RegisterCallback(const OpId& op_id,
                                       const shared_ptr<FutureCallback>& callback) {
  DCHECK(callback);
  callbacks_.insert(CallbackMap::value_type(op_id, callback));
}

void OpIdWaiterSet::MarkFinished(const OpId& op_id,
                                 MarkFinishedMode mode) {
  if (PREDICT_TRUE(callbacks_.empty())) {
    return;
  }

  CallbackMap::iterator iter;
  switch (mode) {
    case MARK_ALL_OPS_BEFORE:
      iter = callbacks_.lower_bound(op_id);
      break;
    case MARK_ONLY_THIS_OP:
      iter = callbacks_.find(op_id);
      break;
  }

  for (; iter != callbacks_.end();) {
    CHECK_OK(callback_pool_->SubmitFunc(boost::bind(&FutureCallback::OnSuccess,
                                                    (*iter).second.get())));
    CallbackMap::iterator tmp = iter;
    ++tmp;
    callbacks_.erase(iter);
    iter = tmp;
  }
}

} // namespace consensus
} // namespace kudu
