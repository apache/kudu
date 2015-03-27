// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_CLIENT_BUILDER_INTERNAL_H
#define KUDU_CLIENT_CLIENT_BUILDER_INTERNAL_H

#include <string>
#include <vector>

#include "kudu/client/client.h"

namespace kudu {

namespace client {

class KuduClientBuilder::Data {
 public:
  Data();
  ~Data();

  std::vector<std::string> master_server_addrs_;
  MonoDelta default_admin_operation_timeout_;
  MonoDelta default_rpc_timeout_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
