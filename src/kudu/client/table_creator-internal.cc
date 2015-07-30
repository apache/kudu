// Copyright (c) 2014, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/table_creator-internal.h"

#include <boost/foreach.hpp>

#include "kudu/gutil/stl_util.h"

namespace kudu {

namespace client {

KuduTableCreator::Data::Data(KuduClient* client)
  : client_(client),
    schema_(NULL),
    num_replicas_(0),
    wait_(true) {
}

KuduTableCreator::Data::~Data() {
  STLDeleteElements(&split_rows_);
}

} // namespace client
} // namespace kudu
