// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_CLIENT_BUILDER_INTERNAL_H
#define KUDU_CLIENT_CLIENT_BUILDER_INTERNAL_H

#include <string>

#include "client/client.h"

namespace kudu {

namespace client {

class KuduClientBuilder::Data {
 public:
  Data();
  ~Data();

  std::string master_server_addr_;
  MonoDelta default_admin_operation_timeout_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
