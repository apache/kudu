// Copyright (c) 2014, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/client_builder-internal.h"

namespace kudu {

namespace client {

KuduClientBuilder::Data::Data()
  : default_admin_operation_timeout_(MonoDelta::FromMilliseconds(5 * 1000)),
    default_select_master_timeout_(MonoDelta::FromMilliseconds(15* 1000)) {
}

KuduClientBuilder::Data::~Data() {
}

} // namespace client
} // namespace kudu
