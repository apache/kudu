// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_TABLE_CREATOR_INTERNAL_H
#define KUDU_CLIENT_TABLE_CREATOR_INTERNAL_H

#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/common/common.pb.h"

namespace kudu {

namespace client {

class KuduTableCreator::Data {
 public:
  explicit Data(KuduClient* client);
  ~Data();

  KuduClient* client_;

  std::string table_name_;

  const KuduSchema* schema_;

  std::vector<const KuduPartialRow*> split_rows_;

  PartitionSchemaPB partition_schema_;

  int num_replicas_;

  MonoDelta timeout_;

  bool wait_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
