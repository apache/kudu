// Copyright (c) 2015, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/tablet_server-internal.h"

using std::string;

namespace kudu {
namespace client {

KuduTabletServer::Data::Data(const string& uuid,
                             const string& hostname)
  : uuid_(uuid),
    hostname_(hostname) {
}

KuduTabletServer::Data::~Data() {
}

} // namespace client
} // namespace kudu
