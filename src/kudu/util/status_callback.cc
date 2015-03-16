// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

namespace kudu {

void DoNothingStatusCB(const Status& status) {}

Status DoNothingStatusClosure() { return Status::OK(); }

} // end namespace kudu
