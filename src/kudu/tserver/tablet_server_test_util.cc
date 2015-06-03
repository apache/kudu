// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tserver/tablet_server_test_util.h"

#include "kudu/consensus/consensus.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"

namespace kudu {
namespace tserver {

using consensus::ConsensusServiceProxy;
using rpc::Messenger;
using std::tr1::shared_ptr;

void CreateTsClientProxies(const Sockaddr& addr,
                           const shared_ptr<Messenger>& messenger,
                           gscoped_ptr<TabletServerServiceProxy>* proxy,
                           gscoped_ptr<TabletServerAdminServiceProxy>* admin_proxy,
                           gscoped_ptr<ConsensusServiceProxy>* consensus_proxy,
                           gscoped_ptr<server::GenericServiceProxy>* generic_proxy) {
  proxy->reset(new TabletServerServiceProxy(messenger, addr));
  admin_proxy->reset(new TabletServerAdminServiceProxy(messenger, addr));
  consensus_proxy->reset(new ConsensusServiceProxy(messenger, addr));
  generic_proxy->reset(new server::GenericServiceProxy(messenger, addr));
}

} // namespace tserver
} // namespace kudu
