// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_TABLET_SERVER_INTERNAL_H
#define KUDU_CLIENT_TABLET_SERVER_INTERNAL_H

#include <string>

#include "kudu/client/client.h"
#include "kudu/gutil/macros.h"

namespace kudu {
namespace client {

class KuduTabletServer::Data {
 public:
  Data(const std::string& uuid,
       const std::string& hostname);
  ~Data();

  const std::string uuid_;
  const std::string hostname_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
