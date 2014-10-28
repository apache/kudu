// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_WRITE_OP_INTERNAL_H
#define KUDU_CLIENT_WRITE_OP_INTERNAL_H

#include "kudu/client/write_op.h"
#include "kudu/common/wire_protocol.pb.h"

namespace kudu {

namespace client {

RowOperationsPB_Type ToInternalWriteType(KuduWriteOperation::Type type);

} // namespace client
} // namespace kudu

#endif
