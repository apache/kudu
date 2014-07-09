// Copyright (c) 2014, Cloudera,inc.

#include "client/error-internal.h"

namespace kudu {

namespace client {

KuduError::Data::Data(gscoped_ptr<KuduWriteOperation> failed_op,
                      const Status& status) :
  failed_op_(failed_op.Pass()),
  status_(status) {
}

KuduError::Data::~Data() {
}

} // namespace client
} // namespace kudu
