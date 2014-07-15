// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_TABLE_CREATOR_INTERNAL_H
#define KUDU_CLIENT_TABLE_CREATOR_INTERNAL_H

#include <string>
#include <vector>

#include "client/client.h"

namespace kudu {

namespace client {

class KuduTableCreator::Data {
 public:
  explicit Data(KuduClient* client);
  ~Data();

  const KuduClient* client_;

  std::string table_name_;

  const KuduSchema* schema_;

  std::vector<std::string> split_keys_;

  bool wait_for_assignment_;

  int num_replicas_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
