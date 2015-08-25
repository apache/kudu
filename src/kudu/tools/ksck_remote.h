// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_TOOLS_KSCK_REMOTE_H
#define KUDU_TOOLS_KSCK_REMOTE_H

#include <string>
#include <vector>

#include "kudu/master/master.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/server/server_base.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tools/ksck.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/net/sockaddr.h"

namespace kudu {

class Schema;

namespace tools {

// This implementation connects to a Tablet Server via RPC.
class RemoteKsckTabletServer : public KsckTabletServer {
 public:
  explicit RemoteKsckTabletServer(const std::string& id,
                                  const Sockaddr& address,
                                  const std::tr1::shared_ptr<rpc::Messenger>& messenger)
      : KsckTabletServer(id),
        address_(address.ToString()),
        messenger_(messenger),
        generic_proxy_(new server::GenericServiceProxy(messenger, address)),
        ts_proxy_(new tserver::TabletServerServiceProxy(messenger, address)) {
  }

  virtual Status Connect() const OVERRIDE;

  virtual Status CurrentTimestamp(uint64_t* timestamp) const OVERRIDE;

  virtual void RunTabletChecksumScanAsync(
      const std::string& tablet_id,
      const Schema& schema,
      const ChecksumOptions& options,
      const ReportResultCallback& callback) OVERRIDE;


  virtual const std::string& address() const OVERRIDE {
    return address_;
  }

 private:
  const std::string address_;
  const std::tr1::shared_ptr<rpc::Messenger> messenger_;
  const std::tr1::shared_ptr<server::GenericServiceProxy> generic_proxy_;
  const std::tr1::shared_ptr<tserver::TabletServerServiceProxy> ts_proxy_;
};

// This implementation connects to a Master via RPC.
class RemoteKsckMaster : public KsckMaster {
 public:

  static Status Build(const Sockaddr& address, std::tr1::shared_ptr<KsckMaster>* master);

  virtual ~RemoteKsckMaster() { }

  virtual Status Connect() const OVERRIDE;

  virtual Status RetrieveTabletServers(TSMap* tablet_servers) OVERRIDE;

  virtual Status RetrieveTablesList(std::vector<std::tr1::shared_ptr<KsckTable> >* tables) OVERRIDE;

  virtual Status RetrieveTabletsList(const std::tr1::shared_ptr<KsckTable>& table) OVERRIDE;

 private:

  explicit RemoteKsckMaster(const Sockaddr& address,
                            const std::tr1::shared_ptr<rpc::Messenger>& messenger)
      : messenger_(messenger),
        proxy_(new master::MasterServiceProxy(messenger, address)) {
  }

  Status GetTableInfo(const std::string& table_name, Schema* schema, int* num_replicas);

  // Used to get a batch of tablets from the master, passing a pointer to the
  // seen last key that will be used as the new start key. The
  // last_partition_key is updated to point at the new last key that came in
  // the batch.
  Status GetTabletsBatch(const std::string& table_name, std::string* last_partition_key,
    std::vector<std::tr1::shared_ptr<KsckTablet> >& tablets, bool* more_tablets);

  std::tr1::shared_ptr<rpc::Messenger> messenger_;
  std::tr1::shared_ptr<master::MasterServiceProxy> proxy_;
};

} // namespace tools
} // namespace kudu

#endif // KUDU_TOOLS_KSCK_REMOTE_H
