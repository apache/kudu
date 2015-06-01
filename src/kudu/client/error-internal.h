// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_ERROR_INTERNAL_H
#define KUDU_CLIENT_ERROR_INTERNAL_H

#include "kudu/client/client.h"
#include "kudu/gutil/gscoped_ptr.h"

namespace kudu {

namespace client {

class KuduError::Data {
 public:
  Data(gscoped_ptr<KuduWriteOperation> failed_op, const Status& error);
  ~Data();

  gscoped_ptr<KuduWriteOperation> failed_op_;
  Status status_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
