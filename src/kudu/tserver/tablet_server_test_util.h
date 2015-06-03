// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TSERVER_TABLET_SERVER_TEST_UTIL_H_
#define KUDU_TSERVER_TABLET_SERVER_TEST_UTIL_H_

#include <tr1/memory>
#include "kudu/gutil/gscoped_ptr.h"

namespace kudu {
class Sockaddr;

namespace consensus {
class ConsensusServiceProxy;
}

namespace rpc {
class Messenger;
}

namespace server {
  class GenericServiceProxy;
}

namespace tserver {
class TabletServerAdminServiceProxy;
class TabletServerServiceProxy;

// Create tablet server client proxies for tests.
void CreateTsClientProxies(const Sockaddr& addr,
                           const std::tr1::shared_ptr<rpc::Messenger>& messenger,
                           gscoped_ptr<TabletServerServiceProxy>* proxy,
                           gscoped_ptr<TabletServerAdminServiceProxy>* admin_proxy,
                           gscoped_ptr<consensus::ConsensusServiceProxy>* consensus_proxy,
                           gscoped_ptr<server::GenericServiceProxy>* generic_proxy);

} // namespace tserver
} // namespace kudu

#endif // KUDU_TSERVER_TABLET_SERVER_TEST_UTIL_H_
