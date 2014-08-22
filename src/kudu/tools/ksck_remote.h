// Copyright (c) 2014, Cloudera, inc.

#ifndef KUDU_TOOLS_KSCK_REMOTE_H
#define KUDU_TOOLS_KSCK_REMOTE_H

#include <string>
#include <vector>

#include "kudu/master/master.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/tools/ksck.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver_service.proxy.h"

namespace kudu {
namespace tools {

// This implementation connects to a Tablet Server via RPC.
class RemoteKsckTabletServer : public KsckTabletServer {
 public:
  RemoteKsckTabletServer(const std::string& uuid, const std::string& address)
      : KsckTabletServer(uuid),
        address_(address) {
  }
  virtual ~RemoteKsckTabletServer() { }

  virtual Status Connect() OVERRIDE;

 private:
  const std::string address_;
  std::tr1::shared_ptr<rpc::Messenger> messenger_;
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> proxy_;
};

// This implementation connects to a Master via RPC.
class RemoteKsckMaster : public KsckMaster {
 public:
  explicit RemoteKsckMaster(const std::string& address)
      : address_(address) {
  }
  virtual ~RemoteKsckMaster() { }

  virtual Status Connect() OVERRIDE;

  virtual Status RetrieveTabletServersList(
      std::vector<std::tr1::shared_ptr<KsckTabletServer> > &tablet_servers) OVERRIDE;

  virtual Status RetrieveTablesList(std::vector<std::tr1::shared_ptr<KsckTable> > &tables) OVERRIDE;

  virtual Status RetrieveTabletsList(const std::tr1::shared_ptr<KsckTable> &table) OVERRIDE;

 private:
  Status GetNumReplicasForTable(const std::string& table_name, int* num_replicas);
  // Used to get a batch of tablets from the master, passing a pointer to the seen last key that
  // will be used as the new start key. The last_key is updated to point at the new last key
  // that came in the batch.
  Status GetTabletsBatch(const std::string& table_name, std::string* last_key,
    std::vector<std::tr1::shared_ptr<KsckTablet> >& tablets, bool* more_tablets);
  const std::string address_;
  std::tr1::shared_ptr<rpc::Messenger> messenger_;
  std::tr1::shared_ptr<master::MasterServiceProxy> proxy_;
};

} // namespace tools
} // namespace kudu

#endif // KUDU_TOOLS_KSCK_REMOTE_H
