// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_TABLE_INTERNAL_H
#define KUDU_CLIENT_TABLE_INTERNAL_H

#include <string>

#include "client/client.h"

namespace kudu {

namespace client {

class KuduTable::Data {
 public:
  Data(const std::tr1::shared_ptr<KuduClient>& client,
       const std::string& name,
       const KuduSchema& schema);
  ~Data();

  Status Open();

  std::tr1::shared_ptr<KuduClient> client_;

  std::string name_;

  // TODO: figure out how we deal with a schema change from the client perspective.
  // Do we make them call a RefreshSchema() method? Or maybe reopen the table and get
  // a new KuduTable instance (which would simplify the object lifecycle a little?)
  const KuduSchema schema_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
