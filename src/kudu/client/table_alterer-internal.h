// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_TABLE_ALTERER_INTERNAL_H
#define KUDU_CLIENT_TABLE_ALTERER_INTERNAL_H

#include <string>

#include "kudu/client/client.h"
#include "kudu/master/master.pb.h"

namespace kudu {

namespace client {

class KuduTableAlterer::Data {
 public:
  explicit Data(KuduClient* client);
  ~Data();

  const KuduClient* client_;

  Status status_;

  master::AlterTableRequestPB alter_steps_;

  MonoDelta timeout_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
