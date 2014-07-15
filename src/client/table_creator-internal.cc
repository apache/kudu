// Copyright (c) 2014, Cloudera,inc.

#include "client/table_creator-internal.h"

namespace kudu {

namespace client {

KuduTableCreator::Data::Data(KuduClient* client)
  : client_(client),
    schema_(NULL),
    wait_for_assignment_(true),
    num_replicas_(0) {
}

KuduTableCreator::Data::~Data() {
}

} // namespace client
} // namespace kudu
