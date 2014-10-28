// Copyright (c) 2014, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/table_alterer-internal.h"

namespace kudu {

namespace client {

KuduTableAlterer::Data::Data(KuduClient* client)
  : client_(client),
    wait_(true) {
}

KuduTableAlterer::Data::~Data() {
}

} // namespace client
} // namespace kudu
