// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_ERROR_INTERNAL_H
#define KUDU_CLIENT_ERROR_INTERNAL_H

#include "client/client.h"

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
