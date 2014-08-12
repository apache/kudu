// Copyright (c) 2014, Cloudera,inc.

#include "kudu/client/table_creator-internal.h"

namespace kudu {

namespace client {

KuduTableCreator::Data::Data(KuduClient* client)
  : client_(client),
    schema_(NULL),
    num_replicas_(0),
    wait_(true) {
}

KuduTableCreator::Data::~Data() {
}

} // namespace client
} // namespace kudu
