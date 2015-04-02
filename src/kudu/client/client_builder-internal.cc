// Copyright (c) 2014, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/client_builder-internal.h"

namespace kudu {

namespace client {

KuduClientBuilder::Data::Data()
  : default_admin_operation_timeout_(MonoDelta::FromSeconds(10)),
    default_rpc_timeout_(MonoDelta::FromSeconds(5)) {
}

KuduClientBuilder::Data::~Data() {
}

} // namespace client
} // namespace kudu
